"""A screenshot guard for this notebook.

Wraps stdout and stderr so that if the API key ever appears in printed output
or in a traceback, it is replaced with a placeholder before it reaches the
screen. Print the whole URL, print the params dict, print the response object,
make any mistake you like -- the key will not render.

Usage, in the first cell of the notebook (after load_dotenv):

    from safety import guard_secrets
    guard_secrets()

This is a safety net for LEARNING, not a security control. It cannot redact
what it does not know about, and it does not survive a kernel restart before
guard_secrets() is called again. The real protections stay what they were:
the key lives in .env, .env is gitignored, and outputs get cleared before any
commit.
"""

import os
import sys

PLACEHOLDER = "<API-KEY-HIDDEN>"


class _RedactingStream:
    def __init__(self, stream, secrets):
        self._stream = stream
        self._secrets = [s for s in secrets if s]

    def write(self, text):
        if isinstance(text, str):
            for s in self._secrets:
                if s in text:
                    text = text.replace(s, PLACEHOLDER)
        return self._stream.write(text)

    def flush(self):
        return self._stream.flush()

    def __getattr__(self, name):
        # Anything we do not override falls through to the real stream.
        return getattr(self._stream, name)


def guard_secrets(*extra_secrets, verbose=True):
    """Redact the API key from anything written to stdout or stderr."""
    secrets = [os.getenv("POLYGON_API_KEY"), *extra_secrets]
    secrets = [s for s in secrets if s]

    if not secrets:
        print("guard_secrets: no key found in the environment -- nothing to guard.")
        print("Did load_dotenv() run first?")
        return

    for name in ("stdout", "stderr"):
        stream = getattr(sys, name)
        if isinstance(stream, _RedactingStream):
            continue
        setattr(sys, name, _RedactingStream(stream, secrets))

    if verbose:
        print(f"Screenshot guard ON. {len(secrets)} secret(s) will render as {PLACEHOLDER}.")
        print("Print anything you like. Screenshots are safe.")
