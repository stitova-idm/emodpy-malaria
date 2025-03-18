#!/usr/bin/env python3
import argparse
import os
import pathlib  # for a join
import sys
from functools import \
    partial  # for setting Run_Number. In Jonathan Future World, Run_Number is set by dtk_pre_proc based on generic param_sweep_value...

# idmtools ...
from idmtools.assets import Asset, AssetCollection  #
from idmtools.builders import SimulationBuilder
from idmtools.core.platform_factory import Platform
from idmtools.entities.experiment import Experiment

# emodpy
import emodpy.emod_task as emod_task
from emodpy.utils import EradicationBambooBuilds
from emodpy.bamboo import get_model_files
from emodpy_malaria.reporters.builtin import ReportVectorGenetics, ReportVectorStats
import emod_api.config.default_from_schema_no_validation as dfs

import manifest

# ****************************************************************
# Features to support:
#
#  Read experiment info from a json file
#  Add Eradication.exe as an asset (Experiment level)
#  Add Custom file as an asset (Simulation level)
#  Add the local asset directory to the task
#  Use builder to sweep simulations
#  How to run dtk_pre_process.py as pre-process
#  Save experiment info to file
# ****************************************************************

"""
    We create a simulation with SpaceSpraying campaign and
    sweep over several parameters of the campaign.

"""


# When you're doing a sweep across campaign parameters, you want those parameters exposed
# in the build_campaign function as done here
def build_campaign(spraying_coverage=0.8,
                   repelling_initial_effect=0.5):
    """
        Creates and returns the full campaign file.
        Campaign file contains all the interventions that will be distributed (ran) over the course of the simulation.
        The file is created on the server and can be modified using the partial and callback functions below for each
        simulation. The exposed parameters are the ones that can be modified at the time of
        campaign file creating
    Args:
        spraying_coverage: coverage of the space spraying
        repelling_initial_effect: initial effect of the spatial and irs repellents

    Returns:
        Configured campaign

    """

    import emod_api.campaign as campaign
    import emodpy_malaria.interventions.spacespraying as spray
    import emodpy_malaria.interventions.ivermectin as ivermectin
    import emodpy_malaria.interventions.irs as irs
    from emodpy_malaria.interventions.usage_dependent_bednet import add_scheduled_usage_dependent_bednet
    # passing in manifest
    campaign.set_schema(manifest.schema_file)
    #add_scheduled_usage_dependent_bednet(campaign, start_day=bednet_start_day, demographic_coverage=bednet_coverage)

    """
    Requirements:  We want a "Node SpatialRepellent that Kills" intervention that affects the vectors that are going to
     try to feed that day. The meal-seeking vectors that are flying around the area all would encounter the spatial 
     repellent, some would be repelled and some would be killed. The vectors that did not get repelled or killed would
      then go on to either feed outdoors or enter the house and feed indoors (or animal feed or artificial diet feed).  
    
    Good News: Node SpatialRepellent repels meal-seeking vectors whether the feed indoors or outdoors (also repells 
    them from animal feeds and atrifical diet)
    
    Problems: Old (solved): Using SpaceSpraying instead of "Node SpatialRepellent that Kills" affects all vectors,
     males and females. This is easily solved by having an insecticide that doesn't affect male vectors.
     
    New problem 1 (solvable): SpaceSpraying does not affect the female vectors that will be looking to feed indoors
     that that day (I guess they live indoors?). This can be worked around by using IRS to kill the indoor-feeding 
     vectors that make it past repelling. 
     
    New problem 2 (un-solvable): However, it also affects all females, meal-seeking and not and there's not a way 
    for us to differentiate between them within the intervention. The closest solution would be to multiply the 
    Initial_Effect of killing by 1/days_between_feeds (+math for indoor-feeding portion), which would reduce the 
    number of killed females to be the same as if we were killing only meal-seeking females, HOWEVER, this would 
    kill out of the entire female population (minus indoor-feeding), rather than from the meal-seeking females (ex:
     100 vectors, 4 days_between_feeds, 0% indoor feeding, 0.5 killing, would kill half the vectors trying to feed 
     on the day, which would be 25 * 0.5 = 12.5; so we can replicate that by saying that our killing = 0.5 * (1/4), 
     but it wouldn't be the same vectors dying, we would be leaving 3/4 of outdoor meal-seeking vectors alive that 
     should be dead).
     
    If my description for the Requirements is correct, we do not have a combination of interventions that can be
     used to replicate "Node SpatialRepellent that Kills" intervention without changes in EMOD. 
    
    """
    # Combo SpaceSpraying and SpatialRepellent and IRS repellent, mimics a spatial repellent that sits by the entrance
    # to the house and repels vectors from biting outdoors and entering the house and also kills some of them.
    # SpatialRepellent repels outdoor-feeding vectors, SpaceSpraying kills ALL the vectors present (male, female,
    # adult, immature) in the node (indoor-feeding and outdoor-feeding) and IRS repellent repels indoor-feeding
    # vectors, to mimic spatial repellent
    # preventing the vectors from entering the house. If IRS killing is turned on, this means that indoor-biting
    # vectors will be subjected to potential death from SpaceSpraying and IRS killing, while outdoor-biting vectors
    # will only be subjected to potential death from SpaceSpraying. Please note, SpaceSpraying kills both males and
    # females and since an outdoor repellent is more likely to kill female vectors looking for a meal, you may want to
    # adjust male death via insecticide resistance and female killing effect.

    start_day = 850
    node_ids = None
    my_insecticide = ""  # an insecticide that kills a lot less males

    # SpaceSpraying kills all vectors in the node, ones seeking to feed and ones not.
    # It might make sense for space spraying to affect 1/Days_Between_Feeds of the female vectors (via spray coverage,
    # initial effect, or insecticide resistance) to mimic females seeking a meal to be more likely to die
    spray.add_scheduled_space_spraying(campaign, start_day=start_day, node_ids=node_ids,
                                       node_property_restrictions=[],
                                       # spray coverage * initial_effect is how many vectors will die
                                       spray_coverage=spraying_coverage * 0.33,  # 1/3 of the vectors are seeking a meal
                                       insecticide=my_insecticide,
                                       killing_initial_effect=0.09,
                                       killing_box_duration=100,
                                       killing_decay_time_constant=-1)

    # repels outdoor-feeding females from feeding outdoors
    spray.add_scheduled_spatial_repellent(campaign, start_day=start_day, node_ids=node_ids,
                                          node_property_restrictions=[],
                                          insecticide=my_insecticide,
                                          spray_coverage=spraying_coverage,
                                          repelling_initial_effect=repelling_initial_effect,
                                          repelling_box_duration=100,
                                          repelling_decay_time_constant=-1)

    # repels indoor-feeding females from feeding inside the house (entering the house)
    irs.add_scheduled_irs_housing_modification(campaign, start_day=start_day, node_ids=node_ids,
                                               insecticide=my_insecticide,
                                               demographic_coverage=spraying_coverage,
                                               repelling_initial_effect=repelling_initial_effect * 0.5,  # because other side of the house not as protected?
                                               repelling_box_duration=100,
                                               repelling_decay_time_constant=-1,
                                               killing_initial_effect=0)


def update_campaign_multiple_parameters(simulation,
                                        spraying_coverage, repelling_initial_effect,
                                        indoor_feeding_fraction,
                                        run_number):
    """
        This is a callback function that updates several parameters in the build_campaign function.
        the sweep is achieved by the itertools creating a an array of inputs with all the possible combinations
        see builder.add_sweep_definition(update_campaign_multiple_parameters function below
    Args:
        simulation: simulation object to which we will attach the callback function
        spraying_coverage: coverage of the space spraying
        repelling_initial_effect: initial effect of the spatial and irs repellents
        indoor_feeding_fraction: fraction of vectors that feed indoors
        run_number: run number

    Returns:
        a dictionary of tags for the simulation to use in COMPS
    """

    build_campaign_partial = partial(build_campaign, spraying_coverage=spraying_coverage,
                                     repelling_initial_effect=repelling_initial_effect)
    simulation.task.create_campaign_from_callback(build_campaign_partial)
    simulation.task.config.parameters.Run_Number = run_number
    simulation.task.config.parameters.Vector_Species_Params[0].Indoor_Feeding_Fraction = indoor_feeding_fraction
    return dict(spraying_coverage=spraying_coverage, repelling_initial_effect=repelling_initial_effect,
                indoor_feeding_fraction=indoor_feeding_fraction, run_number=run_number)


def set_config_parameters(config):
    """
    This function is a callback that is passed to emod-api.config to set parameters The Right Way.
    """
    # sets "default" malaria parameters as determined by the malaria team
    import emodpy_malaria.malaria_config as malaria_config
    config = malaria_config.set_team_defaults(config, manifest)
    malaria_config.add_species(config, manifest, ["gambiae"])
    malaria_config.set_max_larval_capacity(config, species_name="gambiae",
                                           habitat_type="CONSTANT",
                                           max_larval_capacity=10000)
    config.parameters.Simulation_Duration = 1000

    return config


def build_demographics():
    """
    Build a demographics input file for the DTK using emod_api.
    Right now this function creates the file and returns the filename. If calling code just needs an asset that's fine.
    Also right now this function takes care of the config updates that are required as a result of specific demog settings. We do NOT want the emodpy-disease developers to have to know that. It needs to be done automatically in emod-api as much as possible.
    TBD: Pass the config (or a 'pointer' thereto) to the demog functions or to the demog class/module.

    """
    import emodpy_malaria.demographics.MalariaDemographics as Demographics  # OK to call into emod-api

    demographics = Demographics.from_template_node(lat=0, lon=0, pop=10000, name=1, forced_id=1)
    return demographics


def general_sim(selected_platform):
    """
        This function is designed to be a parameterized version of the sequence of things we do
    every time we run an emod experiment.
    Returns:
        Nothing
    """

    # create EMODTask
    print("Creating EMODTask (from files)...")
    task = emod_task.EMODTask.from_default2(
        config_path="config.json",
        eradication_path=manifest.eradication_path,
        campaign_builder=None,
        schema_path=manifest.schema_file,
        ep4_custom_cb=None,
        param_custom_cb=set_config_parameters,
        demog_builder=build_demographics
    )

    # Set platform
    # use Platform("SLURMStage") to run on comps2.idmod.org for testing/dev work
    platform = Platform("Calculon", node_group="idm_48cores", priority="Highest")
    task.set_sif(manifest.sif_id)
    if selected_platform.upper().startswith("SLURM_LOCAL"):
        # This is for native slurm cluster
        # Quest slurm cluster. 'b1139' is guest partition for idm user. You may have different partition and acct
        platform = Platform(selected_platform, job_directory=manifest.job_directory, partition='b1139', time='10:00:00',
                            account='b1139', modules=['singularity'], max_running_jobs=10)
        # set the singularity image to be used when running this experiment
        task.set_sif(manifest.sif_path_slurm, platform)

    # Create simulation sweep with builder
    # sweeping over start day AND killing effectiveness - this will be a cross product
    builder = SimulationBuilder()

    # this will sweep over the entire parameter space in a cross-product fashion
    # you will get 2x3x2 simulations
    builder.add_multiple_parameter_sweep_definition(
        update_campaign_multiple_parameters,
        dict(
            spraying_coverage=[0.8],
            repelling_initial_effect=[0.45],
            indoor_feeding_fraction=[1, 0.75, 0.5, 0.25, 0],
            run_number=range(10)
        )
    )

    # create experiment from builder
    experiment = Experiment.from_builder(builder, task, name="Campaign Sweep, SpaceSpraying")

    # The last step is to call run() on the ExperimentManager to run the simulations.
    experiment.run(wait_until_done=True, platform=platform)

    # Check result
    if not experiment.succeeded:
        print(f"Experiment {experiment.id} failed.\n")
        exit()

    print(f"Experiment {experiment.id} succeeded.")

    # Save experiment id to file
    with open("experiment_id", "w") as fd:
        fd.write(experiment.id)
    print()
    print(experiment.id)


if __name__ == "__main__":
    import emod_malaria.bootstrap as dtk
    import pathlib

    dtk.setup(pathlib.Path(manifest.eradication_path).parent)
    selected_platform = "COMPS"
    general_sim(selected_platform)
