class _Individual:
    def __init__(self, **kwargs):
        self.data = {}
        for key, value in kwargs.items():
            setattr(self, key, value)


class Population(list):
    @classmethod
    def new(cls, *args):
        payload = dict(zip(args[::2], args[1::2]))
        x_rows = list(payload.get("X", []))
        population = cls()
        for idx, row in enumerate(x_rows):
            individual = _Individual(
                X=row,
                F=payload.get("F", [None] * len(x_rows))[idx] if "F" in payload else None,
                G=payload.get("G", [None] * len(x_rows))[idx] if "G" in payload else None,
            )
            population.append(individual)
        return population

    def get(self, name):
        return [getattr(individual, name) for individual in self]
