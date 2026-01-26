#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2025-11-11.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------

import argparse
import multiprocessing
import json
import re
import sys
import os
import gzip
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed


class Multimap(dict):
    def __setitem__(self, key, value):
        if key not in self:
            dict.__setitem__(self, key, [value])  # call super method to avoid recursion
        else:
            self[key].append(value)


class attrdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


class string(str):
    def extract(self, rexp):
        return re.match(rexp, self).group(1)

    def json(self):
        return json.loads(self, object_hook=attrdict)


def run(command, compressed=False, **kwargs):
    """
    Helper for git commands.
    Note: We do not use this for the main clang-tidy execution anymore
    to allow for streaming output.
    """
    if "input" in kwargs:
        input_data = kwargs["input"]

        if type(input_data) is list:
            input_data = "\n".join(input_data) + "\n"

        kwargs["input"] = input_data.encode("utf-8")

    reply = subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
    )

    if compressed:
        stdout = gzip.decompress(reply.stdout)
    else:
        stdout = reply.stdout

    stdout = (
        string(stdout.decode("utf-8", errors="ignore").strip())
        if stdout is not None
        else ""
    )
    stderr = (
        string(reply.stderr.decode("utf-8").strip()) if reply.stderr is not None else ""
    )

    if stderr != "":
        print(stderr, file=sys.stderr)

    return reply.returncode, stdout, stderr


def get_filename(filename):
    return os.path.basename(filename)


def get_fileextn(filename):
    split = os.path.splitext(filename)
    if len(split) <= 1:
        return ""

    return split[-1]


def script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))


def input_files(files):
    if len(files) == 1 and files[0] == "-":
        return [file.strip() for file in sys.stdin.readlines()]
    else:
        return files


def get_all_files(directory, extensions):
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            if any(filename.endswith(ext) for ext in extensions):
                files.append(os.path.join(root, filename))
    return files


def git_changed_lines(commit):
    file = ""
    changed_lines = Multimap()
    returncode, stdout, stderr = run(f"git diff --text --unified=0 {commit}")

    for line in stdout.splitlines():
        line = line.rstrip("\n")
        fields = line.split()

        match = re.match(r"^\+\+\+ b/.*", line)
        if match:
            file = ""

        match = re.match(r"^\+\+\+ b/(.*(\.c|\.cc|\.cpp|\.cxx|\.h|\.hpp|\.hxx))$", line)
        if match:
            file = match.group(1)

        match = re.match(r"^@@", line)
        if match and file != "":
            lspan = fields[2].replace("+", "").split(",")
            start_line = int(lspan[0])
            count = int(lspan[1]) if len(lspan) > 1 else 1
            if count > 0:
                changed_lines[file] = [start_line, start_line + count - 1]

    return changed_lines


def normalize_path(path):
    """Normalize path to be relative to git root or absolute"""
    if os.path.isabs(path):
        return path
    return os.path.normpath(path)


def check_output(output):
    if not output.strip():
        return True
    return re.search(r": (warning|error): ", output) is None


def run_clang_tidy_batch(cmd_base, file_batch):
    """
    Worker function to run clang-tidy on a small batch of files.
    """
    full_cmd = cmd_base + file_batch
    try:
        proc = subprocess.run(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,  # Return strings, not bytes
            encoding="utf-8",
            errors="ignore",
        )
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as e:
        return 1, "", str(e)


def process_gha_output(stdout):
    in_gha = os.environ.get("GITHUB_ACTIONS") is not None
    if in_gha and stdout:
        clang_tidy_pattern = (
            r"^(.*):(\d+):(\d+):\s+(error|warning):\s+(.*) $([a-z0-9,\-]+)$\s*$"
        )
        for stdout_line in stdout.split("\n"):
            m = re.match(clang_tidy_pattern, stdout_line)
            if m:
                file_path, line, col, severity, message, rule = m.groups()
                print(
                    f"::{severity} file={file_path},line={line},col={col},title={rule}::{message}"
                )


def tidy(args):
    extensions = (".c", ".cc", ".cpp", ".cxx", ".h", ".hpp", ".hxx")
    candidate_files = []
    # get file list to check
    if args.directory:
        print(f"Scanning directory: {args.directory} ...")
        candidate_files = get_all_files(args.directory, extensions)
    else:
        candidate_files = input_files(args.files)
        candidate_files = [f for f in candidate_files if f.endswith(extensions)]

    if not candidate_files:
        print("No valid C/C++ files found to check.")
        return 0
    # get build path
    candidate_files = [normalize_path(f) for f in candidate_files]

    if args.exclude:
        try:
            exclude_re = re.compile(args.exclude)
            before_count = len(candidate_files)
            # Filter files out if the regex matches anywhere in the path
            candidate_files = [f for f in candidate_files if not exclude_re.search(f)]

            excluded_count = before_count - len(candidate_files)
            if excluded_count > 0:
                print(
                    f"Excluded {excluded_count} files based on pattern '{args.exclude}'"
                )

            if not candidate_files:
                print("All files were excluded by the filter pattern.")
                return 0

        except re.error as e:
            print(
                f"Error: Invalid regular expression for --exclude: {e}", file=sys.stderr
            )
            return 1

    build_path = args.p or os.getenv("BUILD_PATH", "_build/Release")
    if not build_path:
        if os.path.isfile("compile_commands.json"):
            build_path = "."
        else:
            print("Error: 'compile_commands.json' not found. Set BUILD_PATH or use -p.")
            return 1

    files_to_process = []
    line_filter_json = ""

    if args.commit:
        changed_lines = git_changed_lines(args.commit)

        final_map = {}
        for f in candidate_files:
            if f in changed_lines:
                final_map[f] = changed_lines[f]
                files_to_process.append(f)

        if not files_to_process:
            print("No changes detected in the provided files relative to commit.")
            return 0

        line_filter_json = json.dumps(
            [{"name": key, "lines": value} for key, value in final_map.items()]
        )
    else:
        files_to_process = candidate_files
        line_filter_json = ""

    clang_tidy_bin = args.clang_tidy_binary

    cmd_base = [clang_tidy_bin]

    if args.config_file:
        cmd_base.append(f"--config-file={args.config_file}")
    else:
        cmd_base.append("--format-style=file")

    cmd_base.append(f"-p={build_path}")

    if args.fix:
        cmd_base.append("--fix")

    cmd_base.append("--quiet")

    if line_filter_json:
        cmd_base.append(f"--line-filter={line_filter_json}")
    jobs = args.jobs if args.jobs else max(1, multiprocessing.cpu_count() // 2)
    chunk_size = 5
    file_chunks = [
        files_to_process[i : i + chunk_size]
        for i in range(0, len(files_to_process), chunk_size)
    ]
    print(
        f"Running {clang_tidy_bin} on {len(files_to_process)} files with {jobs} jobs (Batch size: {chunk_size})..."
    )
    final_exit_code = 0

    with ThreadPoolExecutor(max_workers=jobs) as executor:
        future_to_chunk = {
            executor.submit(run_clang_tidy_batch, cmd_base, chunk): chunk
            for chunk in file_chunks
        }

        for future in as_completed(future_to_chunk):
            code, stdout, stderr = future.result()

            if code != 0:
                final_exit_code = 1
            if not check_output(stdout):
                final_exit_code = 1

            if stdout.strip():
                process_gha_output(stdout)
                print(stdout, end="" if stdout.endswith("\n") else "\n")

            if stderr.strip():
                print(stderr, file=sys.stderr)

    return final_exit_code


def parse_args():
    parser = argparse.ArgumentParser(description="Clang Tidy Wrapper Script")

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--commit",
        help="Git commit/ref to compare against (incremental check on changed lines)",
    )
    group.add_argument(
        "--directory", "-d", help="Run recursively on all C/C++ files in this directory"
    )

    parser.add_argument("--fix", action="store_true", help="Automatically apply fixes")
    parser.add_argument(
        "-p", help="Path containing 'compile_commands.json' (build directory)"
    )
    parser.add_argument(
        "--config-file", help="Path to specific .clang-tidy configuration file"
    )
    parser.add_argument(
        "--exclude",
        default="tests/|.*Test\.cpp$|benchmark/|benchmarks/|.*Benchmark\.cpp$|.*Benchmarks\.cpp$|test/",
        help="Regular expression to exclude files or paths (e.g. 'tests/|.*Test\.cpp$')",
    )
    parser.add_argument(
        "files",
        metavar="FILES",
        nargs="*",
        help="Specific files to process (space separated)",
    )

    parser.add_argument("-j", "--jobs", type=int, help="Parallel jobs")

    parser.add_argument(
        "--clang-tidy-binary",
        default="clang-tidy",
        help="Path to clang-tidy binary (default: clang-tidy)",
    )

    return parser.parse_args()


def main():
    try:
        sys.exit(tidy(parse_args()))
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
