#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging

from airbyte_cdk.destinations.vector_db_based.test_utils import BaseIntegrationTest
from airbyte_cdk.models import DestinationSyncMode, Status
from destination_astra.destination import DestinationAstra


class AstraIntegrationTest(BaseIntegrationTest):

    def test_check_valid_config(self):
        outcome = DestinationAstra().check(logging.getLogger("airbyte"), self.config)
        assert outcome.status == Status.SUCCEEDED

    def test_check_invalid_config(self):
        invalid_config = self.config 

        invalid_config["embedding"]["openai_key"] = 123

        outcome = DestinationAstra().check(
            logging.getLogger("airbyte"), invalid_config)
        assert outcome.status == Status.FAILED


