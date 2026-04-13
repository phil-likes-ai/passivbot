from types import SimpleNamespace

import numpy as np


def minimize(problem, algorithm, termination, seed=1, verbose=False):
    sampling = np.asarray(algorithm.initialization.sampling, dtype=np.float64)
    if getattr(algorithm, "repair", None) is not None:
        sampling = np.asarray(algorithm.repair._do(problem, sampling), dtype=np.float64)

    if getattr(problem, "elementwise_runner", None) is not None:
        payloads = problem.elementwise_runner(None, sampling)
        f = np.asarray([payload["F"] for payload in payloads], dtype=np.float64)
        x = sampling[: len(payloads)]
        return SimpleNamespace(F=f, X=x)

    f_rows = []
    for row in sampling:
        out = {}
        problem._evaluate(row, out)
        f_rows.append(out["F"])
    return SimpleNamespace(F=np.asarray(f_rows, dtype=np.float64), X=sampling)
