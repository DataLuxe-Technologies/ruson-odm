try:
    from redbb_driver import bindings

    print("Tried from redbb_driver import bindings")
except:
    pass

try:
    from redbb_driver.bindings.database import *

    print("Tried from redbb_driver.bindings.database import *")
except:
    pass

try:
    import redbb_driver

    print("Tried import redbb_driver")
except:
    pass

try:
    from redbb_driver import redbb_driver

    print("Tried from redbb_driver import redbb_driver")
except:
    pass

from .client import create_client

binginds = redbb_driver.bindings

__doc__ = redbb_driver.__doc__
if hasattr(redbb_driver, "__all__"):
    __all__ = redbb_driver.__all__
