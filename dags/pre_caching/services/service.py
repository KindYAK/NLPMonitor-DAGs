def pre_cache(**kwargs):
    from django.contrib.auth.models import User
    from django.core.cache import cache
    from django.core.cache.utils import make_template_fragment_key
    from django.test.client import Client

    from dashboard.models import DashboardPreset, Widget

    c = Client(HTTP_USER_AGENT='Mozilla/5.0')
    user = User.objects.filter(is_superuser=True).first()
    c.force_login(user)
    calculated_widgets = set()
    for dashboard in DashboardPreset.objects.all():
        print("!!!", "Calc Dashboard ID =", dashboard.id)
        widgets_ids = set([w.id for w in dashboard.widgets.all()])
        updated_widget_ids = widgets_ids - calculated_widgets
        calculated_widgets = calculated_widgets.union(widgets_ids)

        for widget_id in updated_widget_ids:
            key = make_template_fragment_key("widget", [widget_id])
            cache.delete(key)

        try:
            c.get(f'/dashboard/{dashboard.id}/')
        except Exception as e:
            print("!!!", "Exception")
            try:
                print("!!!", e)
            except:
                pass
        else:
            print("!!! Sucess")
