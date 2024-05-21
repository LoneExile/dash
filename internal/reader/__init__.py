from pkg.config import cfg


class Reader:
    def __init__(self):
        self.books_loc = cfg.Books.Location
        self.playbook = cfg.Books.Play
