from .etl_property01 import fload_property01
from .etl_property02 import fload_property02
from .etl_property03 import fload_property03


def fload():
    fload_property01()
    fload_property02()
    fload_property03()


if __name__ == "__main__":
    fload()
