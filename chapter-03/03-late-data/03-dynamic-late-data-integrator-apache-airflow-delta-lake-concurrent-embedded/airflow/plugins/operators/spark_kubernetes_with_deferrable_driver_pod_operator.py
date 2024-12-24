from typing import Any

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.triggers.pod import ContainerState
from airflow.utils.context import Context


class SparkKubernetesOperatorWithDeferrableDriverPodOperator(SparkKubernetesOperator):
    """
    Overrides the default `SparkKubernetesOperator` since the latter one seems not detecting the
    deferred driver correctly, i.e. the deferring quits as soon as the main container completes.
    The main container for the job is not the driver.
    The solution is a bit hacky, though.
    """
    def trigger_reentry(self, context: Context, event: dict[str, Any]) -> Any:
        self.log.info(f'Maybe overwriting the event {event}...')
        try:
            pod_name = event["name"]+'-driver'
            pod_namespace = event["namespace"]
            driver_pod = self.hook.get_pod(pod_name, pod_namespace)
            statuses_to_check = driver_pod.status.container_statuses
            if getattr(statuses_to_check[0].state, ContainerState.RUNNING):
                self.log.info('...overwriting the event as driver is still running; deferring the operator')
                # Let's overwrite the status, as the driver is still running so that the deferrable operator
                # remains awaken
                event['status'] = 'running'
                return super().trigger_reentry(context, event)
            else:
                self.log.info('...not overwriting the event as the driver pod seems not be running.')
                return super().trigger_reentry(context, event)
        except Exception as e:
            self.log.error('An error occured while gettint the driver state', e)
            return super().trigger_reentry(context, event)