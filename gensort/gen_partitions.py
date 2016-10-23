# -*- coding: UTF-8 -*-
import argparse
import os
import shlex
import subprocess
import sys


def main(argv):
    command = ""
    path = os.path.dirname(os.path.abspath(__file__))

    if sys.platform.startswith('win'):
        command = os.path.join(path, "gensort.exe") + " -a -b%d %d %s\partition%d"

    elif sys.platform.startswith('linux'):
        command = os.path.join(path, "gensort") + " -a -b%d %d %s/partition%d"

    parser = argparse.ArgumentParser(description='Generates partitions')
    parser.add_argument('chunks', type=int, help='# of chunks')
    parser.add_argument('-s', '--start', type=int, help='Beginning record in terms of chunks (default:0)',
                        default=0)
    parser.add_argument('-b', '--block', type=int, help='Block size (default:320000)', default=320000)
    parser.add_argument('-o', '--output', help='Output path', default=".")
    args = parser.parse_args()

    chunks = args.chunks
    start_record = args.start
    block_size = args.block
    out_path = args.output
    print '%d chunks, block size %d, outpath %s' % (chunks, block_size, out_path)

    for i in range(start_record, start_record + chunks):
        cmd = command % (i * block_size, block_size, out_path, i)

        if sys.platform.startswith('win'):
            cmd = cmd.encode('string_escape').replace("\\", r"\\")

        print cmd

        args = shlex.split(cmd)
        p = subprocess.Popen(args, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, shell=False)

        for line in p.stdout.readlines():
            print line,
        retval = p.wait()


if __name__ == "__main__":
    main(sys.argv)
