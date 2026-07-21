# The "Experiment API" is just another type of learner.

import asyncio
from collections import defaultdict
import time

from rose.al.experiment import ExperimentLearner, ExperimentDataType, ExperimentRuntime, ExperimentRuntimeClient
from rose.al.active_learner import SequentialActiveLearner
from radical.asyncflow import WorkflowEngine
from radical.asyncflow.streaming import (
    StreamBackend,
)


class MyInputData_T(ExperimentDataType):
    pass

class MyOutputData_T(ExperimentDataType):
    pass

async def rose_test():
    engine = # ....

    asyncflow = await WorkflowEngine.create(engine)
    my_experiment = ExperimentLearner(asyncflow, MyInputData_T, MyOutputData_T)



    acl = SequentialActiveLearner(asyncflow)
    mode_candidate = runtime.new_model_candidate(acl)

    ##################################################################
    # A definition of one "candidate" model for the experiment
    # Uses standard active learning
    ##################################################################

    # Define and register the simulation task
    @acl.simulation_task
    async def simulation(*args, task_description={"shell": True}):
        return f"code/sim.py"

    # Define and register the training task
    @acl.training_task
    async def training(*args, task_description={"shell": True}):
        return f"code/train.py"

    # Define and register the active learning task
    @acl.active_learn_task
    async def active_learn(*args, task_description={"shell": True}):
        return f"code/active.py"

    # Defining the stop criterion with a metric (MSE in this case)
    @acl.as_stop_criterion(metric_name=MEAN_SQUARED_ERROR_MSE, threshold=0.1)
    async def check_mse(*args, task_description={"shell": True}):
        return f"code/check_mse.py"


    @mode_candidate.learning_entry_point
    async def active_learn():
        # House the active learning process in an experiment model
        async for state in acl.start():
            print(f"Iteration {state.iteration}: metric={state.metric_value}")

    @mode_candidate.inference_task
    async def inference():
        # house how to run inference for the model
        return model()


    
    ##################################################################
    # The callback and multi-model training loop 
    ##################################################################


    # signal and event - managed by the experiment runtime.
    signal = my_experiment.create_event()


    @my_experiment.on_input_data
    async def on_input(data: MyInputData_T):
        signal.set()
        print(f"I received: {data}")
    
    @my_experiment.on_output_data
    async def on_output(data: MyOutputData_T):
        print(f"I sent: {data}")


    @my_experiment.main_loop
    async def main_loop():
        # here is where cross learning will go.
        # It is a continual loop running async....
        # It is not a typical task.
        #
        # It's role is to:
        # - evaluate current models
        # - determine what models / items it should refine.


        while True:
            await signal.wait()


            models = my_experiment.get_models()


            scores = defaultdict(float)
            for model in models:
                scores[model] = model.evaluate()
           
            sorted_d = dict(sorted(scores.items(), key=lambda score: score[1]))


            # tell the inference task of the experiment to use the best oner
            async def inference_selector():
                  return sorted_d[0]
            my_experiment.update_inference_selector(inference_selector)

            # pick the worst model and refine it!
            await my_experiment.launch(active_learn, sorted_d[-1])
            # or async...
            new_model = await asyncio.create_task(my_experiment.launch(active_learn,              sorted_d[-1]))


            my_experiment.publish_model(new_model)
       


    #####################################
    # On inference,
    # the runtime will automatically use
    #       runtime.experiment.inference_selector().inference_task()
    #
    # runtime.inference_model is of type model candidate.
    #
    ####################################


    #########################################################
    # Now, the experiment flow .... putting it all together
    #########################################################

    sb = StreamBackend()
    runtime = ExperimentRuntime(asyncflow, stream_backend=sb)

    @runtime.sensor_task(tx_topic="/my_sensor")
    def read_from_sensor():
        sb = StreamBackend()
        runtime_client = ExperimentRuntimeClient(sb)
        while True:
            out = # ... something here.
            runtime_client.dispatch("/my_sensor",out)
            time.sleep(1)

    @runtime.utility_task
    def data_sink(data: MyOutputData_T):
        print(data)
    
    my_experiment.add_upstream(read_from_sensor)
    my_experiment.add_downstream(data_sink)
    
    runtime.add_experiment(my_experiment)

    runtime.run()






    


