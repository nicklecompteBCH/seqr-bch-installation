from architecture.generic.machine import VirtualMachine
from enum import Enum
from typing import Union, List, Dict

class OperatingSystem(Enum):
    Windows = 1,
    MacOS = 2,
    CentOS = 3,
    Debian = 4,
    AWSLinux = 5,
    RHEL = 6

def is_linuxlike(testos: OperatingSystem):
    if testos == OperatingSystem.Windows:
        return False
    return True

def is_centoslike(testos: OperatingSystem):
    testos in {OperatingSystem.AWSLinux, OperatingSystem.CentOS, OperatingSystem.RHEL}

class StandardCommandLineOperations(Enum):
    Sudo = 1,
    SudoSu = 2,
    Ls = 3,
    Copy = 4,
    Move = 5,
    DeleteFile = 6,
    DeleteFolder = 7
    ManagedPackageInstall = 8
    Python = 9
    PythonModule = 10
    Download = 11
    GetCPUUsage = 12,
    GetMemoryUsage = 13,
    GetHardDiskUsage = 14,
    ExportSessionVariable = 15

    def __str__(self):
        if self == StandardCommandLineOperations.Sudo:
            return 'sudo'
        if self == StandardCommandLineOperations.SudoSu:
            return 'sudo -su'
        if self == StandardCommandLineOperations.Ls:
            return 'ls'
        if self == StandardCommandLineOperations.Copy:
            return 'cp'
        if self == StandardCommandLineOperations.DeleteFile:
            return 'rm'
        if self == StandardCommandLineOperations.DeleteFolder:
            return 'rm -rf'
        if self == StandardCommandLineOperations.ManagedPackageInstall:
            return 'yum install'
        if self == StandardCommandLineOperations.Python:
            return "python3"
        if self == StandardCommandLineOperations.PythonModule:
            return "python3 -m"
        if self == StandardCommandLineOperations.Download:
            return "wget"
        if self == StandardCommandLineOperations.ExportSessionVariable:
            return "export"
        else:
            raise ValueError(f"Did not implement case {self}")

    def str_os_specific(self,opsys: OperatingSystem):
        if opsys == OperatingSystem.CentOS:
            return str(self)
        elif is_linuxlike(opsys):
            if self == StandardCommandLineOperations.ManagedPackageInstall:
                if opsys == OperatingSystem.MacOS:
                     return "brew install"
                elif opsys == OperatingSystem.Debian:
                    return "apt-get install"
                else:
                    raise NotImplementedError()
            else:
                return str(self)
        elif opsys == OperatingSystem.Windows:
            raise NotImplementedError()

class TerminalCommand:

    def __init__(
        self,
        operation: Union[StandardCommandLineOperations, str],
        targets: List[str],
        options : Dict[str,str],
        valid_OS: Union[OperatingSystem, None] = None,
        provided_string_rep: str = "",
        linux_generic : bool = False
    ):
        self.operation = operation
        self.targets = targets
        self.options = options
        self.valid_OS = valid_OS
        self.linux_generic = linux_generic
        self.provided_string_rep = provided_string_rep

    def __str__(self):

        if self.provided_string_rep:
            return self.provided_string_rep

        retstr = ""

        if isinstance(self.operation, str):
            retstr = retstr + self.operation
        else:
            if self.valid_OS:
                retstr = retstr + self.operation.str_os_specific(self.valid_OS)
            else:
                retstr = retstr + str(self.operation)

        retstr = retstr + " "

        for targ in self.targets:
            retstr = retstr + targ + " "

        for option,value in self.options.items():
            retstr = retstr + option + " " + value + " "

        return retstr

def export_variable(
    variable_name:str,
    variable_value: str,
    os_type: Union[OperatingSystem,None] = None
):
    return TerminalCommand(
        StandardCommandLineOperations.ExportSessionVariable,
        [f"{variable_name}={variable_value}"],
        {},
        valid_OS=os_type,
        linux_generic=True
    )