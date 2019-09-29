from enum import Enum

# Validation helpers

def validate_percentage(percent:float, name:str = "percent"):
    """Makes sure that a percentage float is between 0 and 1.
    This is intended to be called early in the constructor of classes that
    have a percentage property, or in functions that take a percentage
    as an argument.

    :param percent: The percentage to check.
    :type percent: float
    :param name: The name of the parameter to check, defaults to "percent"
    :type name: str, optional
    :raises ValueError: Raises a ValueError if the percentage is invalid.

    :returns: void
    """
    if percent_used < 0.0 or percent_used > 1.0:
            raise ValueError(f"{name} was supplied {percent}, which is an invalid percentage")


def validate_nonnegativet(value:float, name:str = "cost"):
    """If a float value is a cost (in dollars/etc) is has to be nonnegative.
    This is intended to be called early in the constructor of classes that
    have a cost property, or in functions that take a cost
    as an argument.

    :param cost: The cost to check
    :type cost: float
    :param name: The name of the parameter to check, defaults to "cost"
    :type name: str, optional
    :raises ValueError: Raises a ValueError if the percentage is invalid.

    :returns: void
    """
    if value < 0.0:
        raise ValueError(f"Negative value provided for {name}: {value}")

def validate_current_and_max(
    current_val, max_val,
    name_current:str = "current", name_max:str = "max"
):
    """If two ordered values are a current_val and max_val,
    the current_val has to be less than the max_val.
    This is intended to be called early in the constructor of classes that
    have current/max properties, or in functions that take a current/max
    as an argument.

    :param current_val: The value of something.
    :type current_val: Any type with <, >, etc.
    :param max_val: The maximimum amount of that something.
    :type maxim: [type]
    :param name_current: The name of the current_val parameter to check,
    defaults to "current"
    :type name_current: str, optional
    :param name_max: The name of the max_val parameter to check,
    defaults to "max"
    :type name_max: str, optional
    :raises ValueError: If the current is greater than the max.

    :returns: void
    """
    if current_val > max_val:
        raise ValueError(f"{name_current} {current_val} was creater than {name_max} {max_val}")

class CPUUsage:
    """Very basic and generic class for describing the usage on a CPU.
    For more specific architectures we could look at cpu_1,...,cpu_x in a dict.
    But the 0.0.0.1 PanickedSeqrDeployment version of this package is more basic,
    heavily AWS/EC2-targeted, and parallel-resource-based utilization will be managed
    in the cluster module.

    :param perecent_used: As a  float between 0 and 1, the % of CPU utilized by
    running processes on the machine.

    :type percent_used: float

    :raises ValueError: The percentage must be between 0.0 and 1.0.
    """

    def __init__(
        self,
        percent_used : float
    ):
        # validate percent_used
        validate_percenage(percent_used,"CPUUsage percent used")
        self.percent_used = percent_used

class RAMUsage:
    """Very basic and generic class for describing the RAM usage on a PC.
    Potentially a graphics card.

    :param current_usage: The amount of RAM the machine is currently using.
    Ideally obtained from a useful OS tool, or alternatively a cloud monitoring platform.
    :type current_usage: int

    :param ram_allotted: The amount of RAM available to the machine.
    :ype ram_allotted: int

    :raises ValueError: An exception is thrown upon construction
    if the following criteria are not met:
        current_usage and computer_total are nonnegative
        current_usage <= computer_total
    """

    def __init__(
        self,
        current_usage : int,
        ram_allotted : int
    ):
        # validation
        validate_nonnegativet(current_usage, "RAMUsage.current_usage")
        validate_nonnegativet(ram_allotted, "RAMUsage.ram_allotted")
        validate_current_and_max(current_usage, ram_allotted,"RAMUsage.current_usage","RAMUsage.ram_allotted")
        self.current_usage = current_usage
        self.ram_allotted = ram_allotted

    def __eq__(self,other):
        if other is not RAMUsage:
            raise InvalidTypeError()

class BoundedCost:

    def __init__(
        self,
        current_cost: float,
        max_cost : float
    ):
        if current_cost < 0.0 or max_cost < 0.0:
            raise ValueError(f"Negative values supplied in constructor for BoundedCost: currenr {current_cost}, max {max_cost}")
        self.current_cost = current_cost
        self.max_cost = max_cost

    def __eq__(self, other):
        if other is not BoundedCost:
            return False



class TimeResolution(Enum):
    Hourly = 1,
    Daily = 2,
    Weekly = 3,
    PerWeekday = 4,
    PerWeekend = 5,
    Monthly = 6,
    Yearly = 7

    def str_upper(self):
        """Upper-case string representation.
        Useful for titling reports, etc ("Daily Usage:")

        :raises ValueError: If there isn't an implementation provided
        (e.g. if you add more cases to the enum)
        """
        if self == TimeResolution.Hourly:
            return "Hourly"
        elif self == TimeResolution.Daily:
            return "Daily"
        elif self == TimeResolution.Weekly:
            return "Weekly"
        elif self == TimeResolution.PerWeekday:
            return "Per Weekday"
        elif self == TimeResolution.PerWeekend:
            return "Per Weekend"
        elif self == TimeResolution.Monthly:
            return "Monthly"
        elif self == TimeResolution.Yearly:
            return "Yearly"
        else:
            raise ValueError(f"str_upper implementation not provided for {self}")

class CostPerUnitTime:

    def __init__(
        self,
        current_cost : float,
        max_cost : float,
        time_unit : TimeResolution
    ):

        self.current_cost = current_cost
        self.max_cost = max_cost
        self.time_unit = time_unit