from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, TypedDict, TypeVar, Union, cast, overload

from dagster._core.pipes.utils import PipesSession
from typing_extensions import NotRequired

if TYPE_CHECKING:
    from mypy_boto3_emr.type_defs import ConfigurationUnionTypeDef as EMRConfigurationUnionTypeDef
    from mypy_boto3_emr_containers.type_defs import (
        ConfigurationUnionTypeDef as EMRContainersConfigurationUnionTypeDef,
    )


C = TypeVar(
    "C", bound=Union["EMRConfigurationUnionTypeDef", "EMRContainersConfigurationUnionTypeDef"]
)


@overload
def add_emr_configuration(
    configurations: Sequence["EMRConfigurationUnionTypeDef"],
    configuration: "EMRConfigurationUnionTypeDef",
    uppercase_keys: Literal[True],
): ...


@overload
def add_emr_configuration(
    configurations: Sequence["EMRContainersConfigurationUnionTypeDef"],
    configuration: "EMRContainersConfigurationUnionTypeDef",
    uppercase_keys: Literal[False],
): ...


def add_emr_configuration(
    configurations: Sequence[C],
    configuration: C,
    uppercase_keys: bool,
) -> list[C]:
    """Add a configuration to a list of EMR configurations, merging configurations with the same classification.

    This is necessary because EMR doesn't accept multiple configurations with the same classification.

    EMR uses uppercase_keys=True, while EMR Containers uses uppercase_keys=False.
    """
    configurations = list(configurations)

    classification_key = "Classification" if uppercase_keys else "classification"
    properties_key = "Properties" if uppercase_keys else "properties"

    for existing_configuration in configurations:
        if existing_configuration.get(
            classification_key
        ) is not None and existing_configuration.get(classification_key) == configuration.get(
            classification_key
        ):
            properties = {**existing_configuration.get(properties_key, {})}
            properties.update(properties)

            inner_configurations = cast(list[C], existing_configuration.get(classification_key, []))

            for inner_configuration in cast(list[C], configuration.get(classification_key, [])):
                add_emr_configuration(
                    inner_configurations,  # type: ignore
                    inner_configuration,  # type: ignore
                    uppercase_keys=uppercase_keys,  # type: ignore
                )

            existing_configuration[properties_key] = properties  # type: ignore
            existing_configuration[classification_key] = inner_configurations  # type: ignore

            break
    else:
        configurations.append(configuration)

    return configurations


def emr_inject_pipes_env_vars(
    session: PipesSession, configurations: Sequence[C], uppercase_keys: bool
) -> list[C]:
    """EMR uses uppercase_keys=True, while EMR Containers uses uppercase_keys=False."""
    classification_key = "Classification" if uppercase_keys else "classification"
    properties_key = "Properties" if uppercase_keys else "properties"
    configurations_key = "Configurations" if uppercase_keys else "configurations"

    pipes_env_vars = session.get_bootstrap_env_vars()

    # add all possible env vars to spark-defaults, spark-env, yarn-env, hadoop-env
    # since we can't be sure which one will be used by the job
    configurations = add_emr_configuration(  # type: ignore
        configurations,  # type: ignore
        {  # type: ignore
            classification_key: "spark-defaults",
            properties_key: {
                f"spark.yarn.appMasterEnv.{var}": value for var, value in pipes_env_vars.items()
            },
        },
        uppercase_keys=uppercase_keys,  # type: ignore
    )

    for classification in ["spark-env", "yarn-env", "hadoop-env"]:
        configurations = add_emr_configuration(  # type: ignore
            configurations,  # type: ignore
            {
                classification_key: classification,  # type: ignore
                configurations_key: [
                    {
                        classification_key: "export",
                        properties_key: pipes_env_vars,
                    }
                ],
            },
            uppercase_keys=uppercase_keys,  # type: ignore
        )

    return configurations  # type: ignore


class WaiterConfig(TypedDict):
    """A WaiterConfig representing the configuration of the waiter.

    Args:
        Delay (NotRequired[int]): The amount of time in seconds to wait between attempts. Defaults to 6.
        MaxAttempts (NotRequired[int]): The maximum number of attempts to be made. Defaults to 1000000
            By default the waiter is configured to wait up to 70 days (waiter_delay*waiter_max_attempts).
            See `Boto3 API Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs/waiter/TasksStopped.html>`_
    """

    Delay: NotRequired[int]
    MaxAttempts: NotRequired[int]
