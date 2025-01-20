import os

from dotenv import load_dotenv

from ppa_simulator.__main__ import main

if __name__ == "__main__":
    load_dotenv()
    main(os.getenv("URI"))
