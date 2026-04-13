from types import SimpleNamespace


class _Survival:
    def do(self, problem, pop, n_survive, **kwargs):
        return type(pop)(pop[:n_survive])


class NSGA3:
    def __init__(self, *, ref_dirs, pop_size=None, sampling=None, crossover=None, mutation=None, repair=None, eliminate_duplicates=True):
        self.ref_dirs = ref_dirs
        self.pop_size = pop_size if pop_size is not None else len(ref_dirs)
        self.sampling = sampling
        self.crossover = crossover
        self.mutation = mutation
        self.repair = repair
        self.eliminate_duplicates = eliminate_duplicates
        self.initialization = SimpleNamespace(sampling=sampling)
        self.survival = _Survival()
