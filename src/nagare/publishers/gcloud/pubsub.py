# --
# Copyright (c) 2008-2024 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

"""The Google cloud pub/sub publisher."""

from functools import partial

from nagare.server import publisher


class Publisher(publisher.Publisher):
    """The Google cloud pub/sub publisher."""

    CONFIG_SPEC = dict(
        publisher.Publisher.CONFIG_SPEC, subscription='string(help="name of the subscription to listen to")'
    )
    has_multi_threads = True

    def __init__(self, name, dist, services_service, **conf):
        services_service(super(Publisher, self).__init__, name, dist, **conf)

        self.subscription = None

    def generate_banner(self):
        banner = super(Publisher, self).generate_banner()
        return banner + ' on subscription `{}`'.format(str(self.subscription))

    def start_handle_request(self, app, services, msg):
        try:
            super(Publisher, self).start_handle_request(app, services, subscription=self.subscription, msg=msg)
        except Exception:  # noqa: S110
            pass

    def _serve(self, app, subscription, services_service, **conf):
        self.subscription = services_service[subscription]

        super(Publisher, self)._serve(app)

        self.subscription.start_consuming(partial(self.start_handle_request, app, services_service))

        return 0
