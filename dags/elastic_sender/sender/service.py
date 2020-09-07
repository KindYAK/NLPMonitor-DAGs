def send_elastic(**kwargs):
    from airflow.models import Variable
    from mainapp.management.commands.build_search import Command
    from mainapp.models import Document

    from_id = Variable.get("send_elastic_from_id", default_var=0)
    to_id = Document.objects.latest('id').id

    # Send_elastic call
    Command().handle(batch_size=1000, from_id=from_id, to_id=to_id)

    Variable.set("send_elastic_from_id", to_id)
    return to_id, Variable.get("send_elastic_from_id", default_var=0)
