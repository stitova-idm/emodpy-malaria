from emod_api import schema_to_class as s2c
from emod_api.interventions import utils
from emodpy_malaria.interventions.common import add_campaign_event

iv_name = "SpaceSpraying"


def add_scheduled_space_spraying(
        campaign,
        start_day: int = 1,
        node_ids: list = None,
        node_property_restrictions: list = None,
        repetitions: int = 1,
        timesteps_between_repetitions: int = 365,
        spray_coverage: float = 1.0,
        insecticide: str = "",
        killing_initial_effect: float = 1,
        killing_box_duration: float = -1,
        killing_decay_time_constant: float = 0,
        intervention_name: str = iv_name,
        cost_to_consumer: float = 0,
        disqualifying_properties: list = None,
        dont_allow_duplicates: bool = False,
        new_property_value: str = ""
):
    """

    Args:
        campaign: campaign object to which the intervention will be added, and schema_path container
        start_day: The day the intervention is given out.
        node_ids: List of nodes to which to distribute the intervention. [] or None, indicates all nodes
            will get the intervention
        node_property_restrictions: A list of dictionaries the **NodeProperty** key:value pairs, as defined in the
            demographics file, that the node in which the individual is currently located must have in order to be
            targeted for the intervention. You can specify AND and OR combinations of key:value pairs with this
            parameter. The key:value pairs in the same dictionary are combined with AND, and the dictionaries in the
            list are combined with OR. For example, if you want to target nodes with the node properties
            [ {"Key1": "Value1", "Key2": "Value2"}, {"Key1": "Value3"} ], this will be interpreted as the node needs
            to have Ke1=Value1 AND Key2=Value2 OR Key1=Value3 to get the intervention.
        repetitions: The number of times an intervention is given, used with timesteps_between_repetitions. -1 means
            the intervention repeats forever. Sets **Number_Repetitions**
        timesteps_between_repetitions: The interval, in timesteps, between repetitions. Ignored if repetitions = 1.
            Sets **Timesteps_Between_Repetitions**
        spray_coverage: The portion of the node that has been sprayed.  This value is multiplied by the current
            efficacy of the WaningEffect
        insecticide: The name of the insecticide defined in <config.Insecticides> for this intervention.
            If insecticides are being used, then this must be defined as one of those values.  If they are not
            being used, then this does not needed to be specified or can be empty string.  It cannot have a
            value if <config.Insecticides> does not define anything.
        intervention_name: The optional name used to refer to this intervention as a means to differentiate it from
            others that use the same class. It’s possible to have multiple UsageDependentBednets interventions
            attached to a person if they have different Intervention_Name values.
        killing_initial_effect: Initial strength of the Killing effect. The effect may decay over time.
        killing_box_duration: Box duration of effect in days before the decay of Killing Initial_Effect.
            -1 indicates effect is indefinite (WaningEffectConstant)
        killing_decay_time_constant: The exponential decay length, in days of the Killing Initial_Effect.
        cost_to_consumer: Per unit cost when distributed
        disqualifying_properties: A list of **NodeProperty** key:value pairs that will prevent an intervention from
            being distributed or applied/updated (persistent interventions will abort/expire in the time step they see
            the change in their node property).
        dont_allow_duplicates: When True, if node already has this intervention with the same **intervention_name**,
            then the node will not receive another copy of the intervention.
        new_property_value: A **NodeProperty** key:value pair that will be assigned when the intervention is distributed.

    Returns:
        None, adds the SpaceSpraying intervention to the campaign

    """
    node_intervention = _space_spraying(campaign=campaign,
                                        spray_coverage=spray_coverage,
                                        insecticide=insecticide,
                                        intervention_name=intervention_name,
                                        killing_initial_effect=killing_initial_effect,
                                        killing_box_duration=killing_box_duration,
                                        killing_decay_time_constant=killing_decay_time_constant,
                                        cost_to_consumer=cost_to_consumer,
                                        disqualifying_properties=disqualifying_properties,
                                        dont_allow_duplicates=dont_allow_duplicates,
                                        new_property_value=new_property_value)
    add_campaign_event(campaign=campaign,
                       start_day=start_day,
                       node_ids=node_ids,
                       repetitions=repetitions,
                       timesteps_between_repetitions=timesteps_between_repetitions,
                       node_intervention=node_intervention,
                       node_property_restrictions=node_property_restrictions)


def add_scheduled_spatial_repellent(
        campaign,
        start_day: int = 1,
        node_ids: list = None,
        repetitions: int = 1,
        node_property_restrictions: list = None,
        timesteps_between_repetitions: int = 365,
        spray_coverage: float = 1.0,
        insecticide: str = "",
        repelling_initial_effect: float = 1,
        repelling_box_duration: float = -1,
        repelling_decay_time_constant: float = 0,
        intervention_name: str = iv_name,
        cost_to_consumer: float = 0,
        disqualifying_properties: list = None,
        dont_allow_duplicates: bool = False,
        new_property_value: str = ""
):
    """

    Args:
        campaign: campaign object to which the intervention will be added, and schema_path container
        start_day: The day the intervention is given out.
        node_ids: List of nodes to which to distribute the intervention. [] or None, indicates all nodes
            will get the intervention
        node_property_restrictions: A list of dictionaries the **NodeProperty** key:value pairs, as defined in the
            demographics file, that the node in which the individual is currently located must have in order to be
            targeted for the intervention. You can specify AND and OR combinations of key:value pairs with this
            parameter. The key:value pairs in the same dictionary are combined with AND, and the dictionaries in the
            list are combined with OR. For example, if you want to target nodes with the node properties
            [ {"Key1": "Value1", "Key2": "Value2"}, {"Key1": "Value3"} ], this will be interpreted as the node needs
            to have Ke1=Value1 AND Key2=Value2 OR Key1=Value3 to get the intervention.
        repetitions: The number of times an intervention is given, used with timesteps_between_repetitions. -1 means
            the intervention repeats forever. Sets **Number_Repetitions**
        timesteps_between_repetitions: The interval, in timesteps, between repetitions. Ignored if repetitions = 1.
            Sets **Timesteps_Between_Repetitions**
        spray_coverage: The portion of the node that has been sprayed.  This value is multiplied by the current
            efficacy of the WaningEffect
        insecticide: The name of the insecticide defined in <config.Insecticides> for this intervention.
            If insecticides are being used, then this must be defined as one of those values.  If they are not
            being used, then this does not needed to be specified or can be empty string.  It cannot have a
            value if <config.Insecticides> does not define anything.
        intervention_name: The optional name used to refer to this intervention as a means to differentiate it from
            others that use the same class. It’s possible to have multiple UsageDependentBednets interventions
            attached to a person if they have different Intervention_Name values.
        repelling_initial_effect: Initial strength of the Repelling effect. The effect may decay over time.
        repelling_box_duration: Box duration of effect in days before the decay of **repelling_initial_effect**.
            -1 indicates effect is indefinite (WaningEffectConstant)
        repelling_decay_time_constant: The exponential decay length, in days of the **repelling_initial_effect**.
        cost_to_consumer: Per unit cost when distributed
        disqualifying_properties: A list of **NodeProperty** key:value pairs that will prevent an intervention from
            being distributed or applied/updated (persistent interventions will abort/expire in the time step they see
            the change in their node property).
        dont_allow_duplicates: When True, if node already has this intervention with the same **intervention_name**,
            then the node will not receive another copy of the intervention.
        new_property_value: A **NodeProperty** key:value pair that will be assigned when the intervention is
            distributed. Formatted "key:value".

    Returns:
        None, adds the SpatialRepellent intervention to the campaign
    """
    node_intervention = _spatial_repellent(campaign=campaign,
                                           spray_coverage=spray_coverage,
                                           insecticide=insecticide,
                                           intervention_name=intervention_name,
                                           repelling_initial_effect=repelling_initial_effect,
                                           repelling_box_duration=repelling_box_duration,
                                           repelling_decay_time_constant=repelling_decay_time_constant,
                                           cost_to_consumer=cost_to_consumer,
                                           disqualifying_properties=disqualifying_properties,
                                           dont_allow_duplicates=dont_allow_duplicates,
                                           new_property_value=new_property_value)
    add_campaign_event(campaign=campaign,
                       start_day=start_day,
                       node_ids=node_ids,
                       repetitions=repetitions,
                       timesteps_between_repetitions=timesteps_between_repetitions,
                       node_intervention=node_intervention,
                       node_property_restrictions=node_property_restrictions)


# SpatialRepellent
def _spatial_repellent(campaign,
                       spray_coverage: float = 1.0,
                       insecticide: str = "",
                       repelling_initial_effect: float = 1,
                       repelling_box_duration: float = -1,
                       repelling_decay_time_constant: float = 0,
                       intervention_name: str = iv_name,
                       cost_to_consumer: float = 0,
                       disqualifying_properties: list = None,
                       dont_allow_duplicates: bool = False,
                       new_property_value: str = ""):
    """
        Configures the node-targeted SpatialRepellent intervention

    Args:
        campaign: campaign object to which the intervention will be added, and schema_path container
        spray_coverage: The portion of the node that has been sprayed.  This value is multiplied by the current
            efficacy of the WaningEffect
        insecticide: The name of the insecticide defined in <config.Insecticides> for this intervention.
            If insecticides are being used, then this must be defined as one of those values.  If they are not
            being used, then this does not needed to be specified or can be empty string.  It cannot have a
            value if <config.Insecticides> does not define anything.
        intervention_name: The optional name used to refer to this intervention as a means to differentiate it from
            others that use the same class. It’s possible to have multiple UsageDependentBednets interventions
            attached to a person if they have different Intervention_Name values.
        repelling_initial_effect: Initial strength of the Repelling effect. The effect may decay over time.
        repelling_box_duration: Box duration of effect in days before the decay of **repelling_initial_effect**.
            -1 indicates effect is indefinite (WaningEffectConstant)
        repelling_decay_time_constant: The exponential decay length, in days of the **repelling_initial_effect**.
        cost_to_consumer: Per unit cost when distributed
        disqualifying_properties: A list of **NodeProperty** key:value pairs that will prevent an intervention from
            being distributed or applied/updated (persistent interventions will abort/expire in the time step they see
            the change in their node property).
        dont_allow_duplicates: When True, if node already has this intervention with the same **intervention_name**,
            then the node will not receive another copy of the intervention.
        new_property_value: A **NodeProperty** key:value pair that will be assigned when the intervention is distributed.

    Returns:
        Configured SpatialRepellent intervention
    """
    schema_path = campaign.schema_path

    intervention = s2c.get_class_with_defaults("SpatialRepelling", schema_path)
    intervention.Intervention_Name = intervention_name
    intervention.Insecticide_Name = insecticide
    intervention.Spray_Coverage = spray_coverage
    intervention.Cost_To_Consumer = cost_to_consumer
    intervention.Disqualifying_Properties = disqualifying_properties
    intervention.Dont_Allow_Duplicates = 1 if dont_allow_duplicates else 0
    intervention.New_Property_Value = new_property_value
    intervention.Killing_Config = utils.get_waning_from_params(schema_path=schema_path,
                                                               initial=repelling_initial_effect,
                                                               box_duration=repelling_box_duration,
                                                               decay_time_constant=repelling_decay_time_constant)

    return intervention


def _space_spraying(campaign,
                    spray_coverage: float = 1.0,
                    insecticide: str = "",
                    killing_initial_effect: float = 1,
                    killing_box_duration: float = -1,
                    killing_decay_time_constant: float = 0,
                    intervention_name: str = iv_name,
                    cost_to_consumer: float = 0,
                    disqualifying_properties: list = None,
                    dont_allow_duplicates: bool = False,
                    new_property_value: str = ""):
    """
        Configures the node-targeted SpaceSpraying intervention

    Args:
        campaign: campaign object to which the intervention will be added, and schema_path container
        spray_coverage: The portion of the node that has been sprayed.  This value is multiplied by the current
            efficacy of the WaningEffect
        insecticide: The name of the insecticide defined in <config.Insecticides> for this intervention.
            If insecticides are being used, then this must be defined as one of those values.  If they are not
            being used, then this does not needed to be specified or can be empty string.  It cannot have a
            value if <config.Insecticides> does not define anything.
        intervention_name: The optional name used to refer to this intervention as a means to differentiate it from
            others that use the same class. It’s possible to have multiple UsageDependentBednets interventions
            attached to a person if they have different Intervention_Name values.
        killing_initial_effect: Initial strength of the Killing effect. The effect may decay over time.
        killing_box_duration: Box duration of effect in days before the decay of Killing Initial_Effect.
            -1 indicates effect is indefinite (WaningEffectConstant)
        killing_decay_time_constant: The exponential decay length, in days of the Killing Initial_Effect.
        cost_to_consumer: Per unit cost when distributed
        disqualifying_properties: A list of **NodeProperty** key:value pairs that will prevent an intervention from
            being distributed or applied/updated (persistent interventions will abort/expire in the time step they see
            the change in their node property).
        dont_allow_duplicates: When True, if node already has this intervention with the same **intervention_name**,
            then the node will not from receive another copy of the intervention.
        new_property_value: A **NodeProperty** key:value pair that will be assigned when the intervention is distributed.

    Returns:
        Configured SpaceSpraying intervention
    """
    schema_path = campaign.schema_path

    intervention = s2c.get_class_with_defaults("SpaceSpraying", schema_path)
    intervention.Intervention_Name = intervention_name
    intervention.Insecticide_Name = insecticide
    intervention.Spray_Coverage = spray_coverage
    intervention.Cost_To_Consumer = cost_to_consumer
    intervention.Disqualifying_Properties = disqualifying_properties
    intervention.Dont_Allow_Duplicates = 1 if dont_allow_duplicates else 0
    intervention.New_Property_Value = new_property_value
    intervention.Killing_Config = utils.get_waning_from_params(schema_path=schema_path,
                                                               initial=killing_initial_effect,
                                                               box_duration=killing_box_duration,
                                                               decay_time_constant=killing_decay_time_constant)

    return intervention


def new_intervention_as_file(campaign, start_day: int = 0, filename: str = "SpaceSpraying.json"):
    """
        Creates a file with SpaceSpray intervention

    Args:
        campaign: campaign object to which the intervention will be added, and schema_path container
        start_day: the day to distribute the SpaceSpraying intervention
        filename: name of the filename created

    Returns:
        filename of the file created
    """
    add_scheduled_space_spraying(campaign=campaign, start_day=start_day)
    campaign.save(filename)
    return filename
