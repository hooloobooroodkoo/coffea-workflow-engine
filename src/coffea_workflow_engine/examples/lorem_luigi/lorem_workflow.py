import json
from pathlib import Path

import luigi
from luigi import LocalTarget

from scripts.split import split_file
from scripts.count_letters import count_letters_file
from scripts.merge_counts import merge_counts


def ensure_dir(p):
    p.mkdir(parents=True, exist_ok=True)


class Split(luigi.Task):
    src = luigi.Parameter(default="lorem.txt")
    chunk_bytes = luigi.IntParameter(default=200)
    chunk_prefix = luigi.Parameter(default="chunk_")
    workdir = luigi.Parameter(default="chunks_inter_files")

    def output(self):
        ensure_dir(Path(self.workdir))
        return LocalTarget(str(Path(self.workdir) / "split_manifest.json"))

    def run(self):
        data_len = len(Path(self.src).read_bytes())
        n_chunks = (data_len + self.chunk_bytes - 1) // self.chunk_bytes

        chunk_paths = [
            str(Path(self.workdir) / f"{self.chunk_prefix}{i:03d}.txt")
            for i in range(n_chunks)
        ]

        split_file(self.src, chunk_paths, self.chunk_bytes)

        manifest = {"n_chunks": n_chunks, "chunks": chunk_paths}
        with self.output().open("w") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)


class CountLetters(luigi.Task):
    src = luigi.Parameter(default="lorem.txt")
    chunk_bytes = luigi.IntParameter(default=200)
    chunk_prefix = luigi.Parameter(default="chunk_")
    workdir = luigi.Parameter(default="chunks_inter_files")
    cid = luigi.Parameter()  # "000", "001", ...

    def requires(self):
        return Split(
            src=self.src,
            chunk_bytes=self.chunk_bytes,
            chunk_prefix=self.chunk_prefix,
            workdir=self.workdir,
        )

    def output(self):
        ensure_dir(Path(self.workdir))
        return LocalTarget(str(Path(self.workdir) / f"counts_{self.cid}.json"))

    def run(self):
        chunk_path = str(Path(self.workdir) / f"{self.chunk_prefix}{self.cid}.txt")
        count_letters_file(chunk_path, self.output().path)


class MergeCounts(luigi.Task):
    src = luigi.Parameter(default="lorem.txt")
    chunk_bytes = luigi.IntParameter(default=200)
    chunk_prefix = luigi.Parameter(default="chunk_")
    workdir = luigi.Parameter(default="chunks_inter_files")
    merged_out = luigi.Parameter(default="letter_counts.json")

    def requires(self):
        return Split(
            src=self.src,
            chunk_bytes=self.chunk_bytes,
            chunk_prefix=self.chunk_prefix,
            workdir=self.workdir,
        )

    def output(self):
        return LocalTarget(self.merged_out)

    def run(self):
        with self.input().open("r") as f:
            manifest = json.load(f)

        n_chunks = int(manifest["n_chunks"])

        # if I wanted to compute all batches parallel if I used more than 1 worker
        count_files = []
        for i in range(n_chunks):
            cid = f"{i:03d}"
            t = CountLetters(
                src=self.src,
                chunk_bytes=self.chunk_bytes,
                chunk_prefix=self.chunk_prefix,
                workdir=self.workdir,
                cid=cid,
            )
            yield t
            count_files.append(t.output().path)

        merge_counts(count_files, self.output().path)


if __name__ == "__main__":
    luigi.run()