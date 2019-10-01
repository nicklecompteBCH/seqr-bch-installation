from generic.vm_os import (
    TerminalCommand,
    StandardCommandLineOperations,
    OperatingSystem
)
from typing import List

def install_java8(os: OperatingSytem) -> List[TerminalCommand] :

    jre_managed = TerminalCommand(
    StandardCommandLineOperations.ManagedPackageInstall,

)