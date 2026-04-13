from types import SimpleNamespace


class _Survival:
    def do(self, problem, pop, n_survive, **kwargs):
        return type(pop)(pop[:n_survive])


class NSGA2:
    def __init__(self, *, pop_size, sampling, crossover=None, mutation=None, repair=None, eliminate_duplicates=True):
        self.pop_size = pop_size
        self.sampling = sampling
        self.crossover = crossover
        self.mutation = mutation
        self.repair = repair
        self.eliminate_duplicates = eliminate_duplicates
        self.initialization = SimpleNamespace(sampling=sampling)
        self.survival = _Survival()
