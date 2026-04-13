class ElementwiseProblem:
    def __init__(self, *, n_var, n_obj, n_ieq_constr=0, xl=None, xu=None, elementwise_runner=None, **kwargs):
        self.n_var = n_var
        self.n_obj = n_obj
        self.n_ieq_constr = n_ieq_constr
        self.xl = xl
        self.xu = xu
        self.elementwise_runner = elementwise_runner
        self.kwargs = kwargs

    def _evaluate(self, x, out, *args, **kwargs):  # pragma: no cover - subclass hook
        raise NotImplementedError
