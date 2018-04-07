import progressbar
import sys


class ProgressBar:
    def __init__(self, text, max_value):
        self.text = text
        if sys.stdout.isatty():
            self.pbar = progressbar.ProgressBar(widgets=["{0:40} ".format(text),
                                                        progressbar.Percentage(), " ",
                                                        progressbar.Bar(), " ",
                                                        progressbar.ETA()]).start(max_value)
        else:
            self.pbar = None
            print(text)

    def next(self):
        if self.pbar is not None:
            self.pbar.update(self.pbar.value + 1)

    def finish(self):
        if self.pbar is not None:
            self.pbar.finish()
