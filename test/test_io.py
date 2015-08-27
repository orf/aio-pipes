from aiopipes.pipeio import FileIO
from io import StringIO
import json


class UnclosableStringIO(StringIO):
    def close(self, *args, **kwargs):
        return


def test_fileio_format(pipeline, run):
    out = UnclosableStringIO()
    inp = UnclosableStringIO()

    def func(d):
        return {'number': d['number'] + 1}

    pipeline = pipeline | func
    pipeline < inp
    pipeline > out

    for i in range(10):
        inp.write(json.dumps({"number": i}) + "\n")

    inp.seek(0)

    run(pipeline.start())

    out.seek(0)

    output = []
    for line in out.readlines():
        output.append(json.loads(line))

    assert output == [{'number': i+1} for i in range(10)]


def test_fileio(tmpdir, pipeline, run):
    out_file = tmpdir.join("output").ensure()

    with out_file.open(mode="w") as fd:
        out = FileIO(fd)

        pipeline = pipeline | (lambda x: x)
        pipeline < range(10)
        pipeline > out

        run(pipeline.start())

    with out_file.open(mode="r") as fd:
        read = [int(l.strip()) for l in fd]
        assert read == list(range(10))
