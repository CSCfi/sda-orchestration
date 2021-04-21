"""Handle registration of DOI in REMS."""

from os import environ
from .logger import LOG

from ..config import CONFIG_INFO

from httpx import Headers, AsyncClient


class REMSHandler:
    """Register Dataset in REMS.

    We will use the DOI of the dataset to create a resource in REMS,
    enable that resource. We will attach the resources as the catalogue item.

    The main function is register resource, that registers resource in REMS under the doi
    used to register at Datacite. We reuse existing resources if they exist.

    The default config should be changed depending per installation, current config is NeIC
    specific.
    """

    def __init__(self) -> None:
        """Define DOI credentials and config.

        :param state: can be publish, register, hide or draft.
        """
        self.rems_api = environ.get("REMS_API", "")
        self.rems_user = environ.get("REMS_USER", "")
        self.rems_key = environ.get("REMS_KEY", "")
        self.config = CONFIG_INFO["rems"]
        self.headers = Headers(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "x-rems-api-key": self.rems_key,
                "x-rems-user-id": self.rems_user,
            }
        )

    async def register_resource(self, doi: str) -> None:
        """Register a resource and its dependencies for making it usable, in REMS.

        To make a resource accessible by a user one needs:

        - Every object in REMS needs to belong to an organisation, so have (at least) one.
            - An organisation needs an internal id, a name and a short name.
        - Every application has to be processed by a workflow, so have (at least) one.
            - A workflow needs to have an organisation, title, type, and list of users
              – handlers – who can process applications.
            - Use default workflow type. Decider type is for e.g. governmental use only.
            - Optionally a workflow can have a form, but generally it is not needed and should
              only be used if absolutely required.
        - Every application must have a form which an applicant can fill in
            - A form must belong to an organisation, but is otherwise a free-form.
        - Every dataset should have one or more licenses.
            - A license needs to have an organisation, type, and a name. Depending on type, it may require more data.
        - Every dataset is registered as a resource.
            - A resource belongs to an organisation, must have an identifier, and should have one or more licenses.
        - Every dataset is findable by a catalog item.
            - A catalog item bundles all of the above together and is an object of interest to the applicants.
            - A catalog item has 1-n mapping: every workflow, form and resource can belong to n catalog items.
        """
        try:
            await self._organization()
            license_id = await self._license()
            form_id = await self._form()
            workflow_id = await self._workflow()

            resource_id = await self._resource(doi, license_id)
            await self._catalogue_item(form_id, resource_id, workflow_id, doi)
        except Exception:
            raise

    async def _process_create(self, resource: str, payload: dict, resp_key: str = "id") -> int:
        """Process creation of a REMS resource endpoint in a similar fashion so that we can retrieve its id."""
        async with AsyncClient() as client:
            response = await client.post(f"{self.rems_api}/api/{resource}/create", json=payload, headers=self.headers)
        if response.status_code == 200:
            _resp = response.json()
            if isinstance(_resp["success"], bool) and _resp["success"]:
                _id = _resp[resp_key]
                LOG.info(f"Created {resource} with id {_id}.")
            else:
                LOG.error(f"Error occurred when creating {resource}.")
                raise Exception(f"Error occurred when creating {resource}.")
        else:
            LOG.error(f"Error occurred when creating {resource} got HTTP status: {response.status_code}")
            raise Exception(f"Error occurred when creating {resource} got HTTP status: {response.status_code}")

        return _id

    async def _organization(self) -> None:
        """Create organization if it does not exist.

        The organization id is established in configuration and we use that to
        assign other resources to it as well
        """
        org_exists = False
        org = self.config["organization"]
        org_payload = {
            "archived": False,
            "enabled": True,
            "organization/id": org["id"],
            "organization/short-name": org["name"],
            "organization/name": org["shortName"],
            "organization/owners": [{"userid": self.rems_user}],
        }

        async with AsyncClient() as client:
            response = await client.get(f"{self.rems_api}/api/organizations/{org['id']}", headers=self.headers)
        if response.status_code == 200:
            org_resp = response.json()
            if org_resp["organization/id"] == org["id"]:
                org_exists = True
                LOG.info(f"Organization with id {org['id']} exists")
        else:
            LOG.error(f"Retrieving organizations failed with HTTP status: {response.status_code}")

        if not org_exists:
            await self._process_create("organizations", org_payload, "organization/id")

    async def _license(self) -> int:
        """Get or create license if one does not exist.

        We check from existing licenses if one exists with the same URL, if yes use that if not
        create a new license with that URL for our organization.
        """
        license_exists = False
        license_id = 0
        license_payload = {
            "licensetype": "link",
            "organization": {"organization/id": self.config["organization"]["id"]},
            "localizations": self.config["license"]["localizations"],
        }

        async with AsyncClient() as client:
            response = await client.get(
                f"{self.rems_api}/api/licenses",
                headers=self.headers,
            )
        if response.status_code == 200:
            license_resp = response.json()
            for lnc in license_resp:
                if (
                    lnc["organization"]["organization/id"] == self.config["organization"]["id"]
                    and lnc["localizations"]["en"]["title"] == self.config["license"]["localizations"]["en"]["title"]
                ):
                    license_exists = True
                    license_id = lnc["id"]
                    LOG.info(
                        f"License {self.config['license']['localizations']['en']['title']} with id {license_id} exists."
                    )
        else:
            LOG.error(f"Retrieving licenses failed with HTTP status: {response.status_code}")

        if not license_exists:
            license_id = await self._process_create("licenses", license_payload)

        return license_id

    async def _workflow(self) -> int:
        """Create base workflow if one does not exist."""
        workflow_exists = False
        workflow_id = 0
        workflow_payload = {
            "organization": {"organization/id": self.config["organization"]["id"]},
            "title": self.config["workflow"]["title"],
            "type": "workflow/default",
            "handlers": [self.rems_user],
        }

        async with AsyncClient() as client:
            response = await client.get(
                f"{self.rems_api}/api/workflows",
                headers=self.headers,
            )
        if response.status_code == 200:
            workflow_resp = response.json()
            for wkf in workflow_resp:
                if (
                    wkf["organization"]["organization/id"] == self.config["organization"]["id"]
                    and wkf["title"] == self.config["workflow"]["title"]
                ):
                    workflow_exists = True
                    workflow_id = wkf["id"]
                    LOG.info(f"Workflow {self.config['workflow']['title']} with id {workflow_id} exists.")
        else:
            LOG.error(f"Retrieving workflows failed with HTTP status: {response.status_code}")

        if not workflow_exists:
            workflow_id = await self._process_create("workflows", workflow_payload)

        return workflow_id

    async def _form(self) -> int:
        """Create a basic form if one does not exist used in the application of a resource."""
        form_exists = False
        form_id = 0
        form_payload = {
            "organization": {"organization/id": self.config["organization"]["id"]},
            "form/title": self.config["form"]["title"],
            "form/fields": self.config["form"]["fields"],
        }

        async with AsyncClient() as client:
            response = await client.get(
                f"{self.rems_api}/api/forms",
                headers=self.headers,
            )
        if response.status_code == 200:
            from_resp = response.json()
            for form in from_resp:
                if (
                    form["organization"]["organization/id"] == self.config["organization"]["id"]
                    and form["form/title"] == self.config["form"]["title"]
                ):
                    form_exists = True
                    form_id = form["form/id"]
                    LOG.info(f"Form {self.config['form']['title']} with id {form_id} exists.")
        else:
            LOG.error(f"Retrieving forms failed with HTTP status: {response.status_code}")
        if not form_exists:
            form_id = await self._process_create("forms", form_payload)

        return form_id

    async def _catalogue_item(self, form_id: int, resource_id: int, workflow_id: int, doi: str) -> None:
        """Create catalogue item to associate a resource to it."""
        item_exists = False
        item_payload = {
            "form": form_id,
            "resid": resource_id,
            "wfid": workflow_id,
            "organization": {"organization/id": self.config["organization"]["id"]},
            "localizations": {
                "en": {
                    "title": f"Catalogue item for resource {doi}",
                    "infourl": doi,
                },
            },
            "enabled": True,
            "archived": False,
        }

        async with AsyncClient() as client:
            response = await client.get(
                f"{self.rems_api}/api/catalogue-items",
                headers=self.headers,
            )
        if response.status_code == 200:
            item_resp = response.json()
            for item in item_resp:
                if (
                    item["organization"]["organization/id"] == self.config["organization"]["id"]
                    and item["wfid"] == workflow_id
                    and item["resid"] == doi
                    and item["formid"] == form_id
                ):
                    item_exists = True
                    LOG.info(f"Catalogue Item for resource with DOI {doi} exists with id {item['id']}.")
        else:
            LOG.error(f"Retrieving catalogue items failed with HTTP status: {response.status_code}")
        if not item_exists:
            await self._process_create("catalogue-items", item_payload)

    async def _resource(self, doi: str, license_id: int) -> int:
        """Create a resource and point it to DataCite DOI."""
        resource_exists = False
        resource_id = 0
        resource_payload = {
            "resid": doi,
            "organization": {"organization/id": self.config["organization"]["id"]},
            "licenses": [license_id],
        }

        async with AsyncClient() as client:
            response = await client.get(
                f"{self.rems_api}/api/resources",
                headers=self.headers,
            )
        if response.status_code == 200:
            resource_resp = response.json()
            for res in resource_resp:
                if res["organization"]["organization/id"] == self.config["organization"]["id"] and res["resid"] == doi:
                    resource_exists = True
                    resource_id = res["id"]
                    LOG.info(f"Resource for DOI {doi} exists with id {resource_id}.")
        else:
            LOG.error(
                f"Retrieving resources failed with HTTP status: {response.status_code} and response: {response.json()}"
            )

        if not resource_exists:
            resource_id = await self._process_create("resources", resource_payload)

        return resource_id

    async def _enable_resource(self, resource_id: int) -> None:
        """Enable a resource in REMS so that it can be used.

        This might not be required, but good to keep arround if a use case presents.
        """
        resource_payload = {"id": resource_id, "enabled": True}
        async with AsyncClient() as client:
            response = await client.put(
                f"{self.rems_api}/api/resources/enabled", json=resource_payload, headers=self.headers
            )
        if response.status_code == 200:
            _resp = response.json()
            if isinstance(_resp["success"], bool) and _resp["success"]:
                LOG.info(f"Enabled resource with id {resource_id}.")
            else:
                LOG.error("Error occurred when enabling resource.")
                raise Exception("Error occurred when enabling resource.")
        else:
            LOG.error(f"Error occurred when enabling resource got HTTP status: {response.status_code}")
            raise Exception(f"Error occurred when enabling resource got HTTP status: {response.status_code}")
