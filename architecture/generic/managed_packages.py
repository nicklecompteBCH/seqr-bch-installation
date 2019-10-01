from enum import Enum
from vm_os import OperatingSystem,TerminalCommand,is_centoslike

class ManagedPackage(Enum):
    Java8
    OpenJDK8

def get_package_name_for_OS(package:ManagedPackage,opsys: OperatingSystem):
    """ Lookup the specific name you need to yum install or brew install, etc.

    :type package: ManagedPackage
    :type opsys: OperatingSystem
    :raises NotImplementedError: For most cases :(
    :rtype: A string, the name of the package repo.
    """
    if package == ManagedPackage.Java8:
        raise NotImplementedError()
    if package == ManagedPackage.OpenJDK8:
        if is_centoslike(opsys):
            return "java-1.8.0-openjdk"

def install_java_8_command(
    os_type: OperatingSystem,
    make_default_java = True,
) -> List[TerminalCommand]:
    package_name = get_package_name_for_OS(ManagedPackage.OpenJDK8, os_type)

