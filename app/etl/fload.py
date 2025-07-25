from .etl_property01 import fload_property01
from .etl_property02 import fload_property02
from .etl_property03 import fload_property03
from .etl_syrena_cruise import fload_syrena_cruise
from .etl_syrena_cruise import iload_syrena_cruise


def fload():
    # fload_property01()
    # fload_property02()
    # fload_property03()
    fload_syrena_cruise()

def iload():
    # iload_property01()
    # iload_property02()
    # iload_property03()
    iload_syrena_cruise()


if __name__ == "__main__":
    fload()
