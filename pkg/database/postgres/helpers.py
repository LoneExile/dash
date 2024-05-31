class Helpers:
    def __init__(self):
        pass

    @staticmethod
    def escape_name(name):
        """Escape a given name."""
        return '"' + '\\"' + name + '\\"' + '"'
