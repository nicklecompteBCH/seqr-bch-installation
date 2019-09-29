from enum import Enum

class CPUUsage:
    """Very basic and generic class for describing the usage on a CPU.
    For more specific architectures we could look at cpu_1,...,cpu_x in a dict.
    But the 0.0.0.1 PanickedSeqrDeployment version of this package is more basic,
    heavily AWS/EC2-targeted, and parallel-resource-based utilization will be managed
    in the cluster module.

    :raises ValueError: The percentage must be between 0.0 and 1.0.
    """

    def __init__(
        self,
        percent_used : float
    ):
        # validate percent_used
        if percent_used < 0.0 or percent_used > 1.0:
            raise ValueError(f"CPUCoreUsage was supplied {percent_used}, which is invalid")

class RAMUsage:
    """Very basic and generic class for describing the RAM usage on a PC.
    Potentially a graphics card.
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
        if max_usage < 0 or current_usage < 0:
            raise ValueError(
                f"RAMUsage was calculated with negative value(s): current {current_usage}, allotted {ram_allotted}"
            )
        if current_usage > max_usage:
            raise ValueError(
                f"RAMUsage was calculated with current usage ({current_usage}) greater than allotted {ram_allotted}"
        )

class Cost:

    def __init__(
        self,
        current_cost: float,
        max_cost : float
    )

class TimeResolution(Enum):
    Hourly = 1,
    Daily = 2,
    Weekly = 3,
    PerWeekday = 4,
    PerWeekend = 5,
    Monthly = 6,
    Yearly = 7

class CostPerUnitTime:

    def __init__(
        self,
        current_cost : float,
        max_cost : float,
        time_unit : TimeResolution
    )