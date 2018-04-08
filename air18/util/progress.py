import progressbar
import sys


class ProgressBar:
    def __init__(self, text, max_value=26):
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

    def update(self, token):
        if self.pbar is not None:
            char = token[0]
            value = ord(char) - 97      # maps a-z to 0-25
            if value > 0:
                self.pbar.update(value)

    def finish(self):
        if self.pbar is not None:
            self.pbar.finish()
