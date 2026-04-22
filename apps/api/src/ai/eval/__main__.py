"""Allow `python -m src.ai.eval ...`."""

from .cli import main
import sys

sys.exit(main())
