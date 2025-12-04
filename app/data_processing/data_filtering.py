from app.utils import extract_date


class ProcessForBigQuery:
    """
    We only keep the required fields for each table
    """

    def clean_workspaces(workspaces: list, timestamp: str) -> list:
        cleaned_workspaces = []
        for workspace in workspaces:
            # https://whoz.stoplight.io/docs/whoz-api/a136fe3b77f57-workspace

            # Initialization of the elements that aren't on the first level of the dict
            address_formatted_address = None
            address_street_address = None
            address_postal_code = None
            address_city = None
            address_country_code = None

            workspace_address = workspace.get("address", {})
            if workspace_address:
                address_formatted_address = workspace_address.get("formattedAddress")
                address_street_address = workspace_address.get("streetAddress1")
                address_postal_code = workspace_address.get("postalCode")
                address_city = workspace_address.get("city")
                address_country_code = workspace_address.get("countryCode")

            cleaned_workspace = {
                "id": workspace.get("id"),
                "time_stamp": timestamp,
                "name": workspace.get("name"),
                "address_formatted_address": address_formatted_address,
                "address_street_address": address_street_address,
                "address_postal_code": address_postal_code,
                "address_city": address_city,
                "address_country_code": address_country_code,
                "public_holidays": workspace.get("publicHolidays"),
                "recruitment_workflow": workspace.get("recruitmentWorkflow"),
                "sales_workflow": workspace.get("salesWorkflow"),
                "staffing_workfow": workspace.get("staffingWorkfow"),
                "status": workspace.get("status"),
            }
            cleaned_workspaces.append(cleaned_workspace)
        return cleaned_workspaces

    def clean_accounts(accounts: list, timestamp: str) -> list:
        cleaned_accounts = []
        for account in accounts:
            # https://whoz.stoplight.io/docs/whoz-api/fe5501ea75a53-account
            cleaned_account = {
                "id": account.get("id"),
                "time_stamp": timestamp,
                "type": account.get("type"),
                "name": account.get("name"),
                "active": account.get("active"),
                "workspace_id": account.get("workspaceId"),
            }
            cleaned_accounts.append(cleaned_account)
        return cleaned_accounts

    def clean_projects(projects: list, timestamp: str) -> list:
        cleaned_projects = []
        for project in projects:
            # https://whoz.stoplight.io/docs/whoz-api/b387fc665661b-project
            cleaned_project = {
                "id": project.get("id"),
                "time_stamp": timestamp,
                "account_id": project.get("accountId"),
                "workspace_id": project.get("workspaceId"),
                "name": project.get("name"),
                "external_id": project.get("externalId"),
                "status": project.get("status"),
                "owner_id": project.get("ownerId"),
            }
            cleaned_projects.append(cleaned_project)
        return cleaned_projects

    def clean_worklogs(worklogs: list, timestamp: str) -> list:
        cleaned_worklogs = []
        for worklog in worklogs:
            # https://whoz.stoplight.io/docs/whoz-api/26a556f3af741-worklog
            cleaned_worklog = {
                "id": worklog.get("id"),
                "time_stamp": timestamp,
                "workspace_id": worklog.get("workspaceId"),
                "talent_id": worklog.get("talentId"),
                "task_id": worklog.get("taskId"),
                "type": worklog.get("type"),
                "workload": worklog.get("workload"),
                "last_modified_by": worklog.get("lastModifiedBy"),
                "last_modified_date": worklog.get("lastModifiedDate"),
                "date": worklog.get("date"),
            }
            cleaned_worklogs.append(cleaned_worklog)
        return cleaned_worklogs

    def clean_talents(talents: list, timestamp: str) -> list:
        cleaned_talents = []
        for talent in talents:
            # https://whoz.stoplight.io/docs/whoz-api/4b21183f6dc28-talent

            # Initialization of the elements that aren't on the first level of the dict
            main_email_address = None
            person_profile_version_name = None
            person_profile_person_id = None
            person_profile_talent_id = None
            person_id = None
            person_first_name = None
            person_last_name = None
            person_main_workspace_id = None
            unit_cost_value = None
            unit_rate_value = None
            person_profile_completion_rate = None
            person_profile_position_start_date = None
            person_profile_position_end_date = None

            person = talent.get("person", {})
            if person:
                person_id = person.get("id")
                person_first_name = person.get("firstName")
                person_last_name = person.get("lastName")
                person_main_workspace_id = person.get("mainWorkspaceId")

                email_addresses = person.get("emails", [])
                for email in email_addresses:
                    main_email_address = email.get("address") if (email.get("main")) else None

                profile = person.get("profile", {})
                if profile:
                    person_profile_version_name = profile.get("versionName")
                    person_profile_person_id = profile.get("personId")
                    person_profile_talent_id = profile.get("talentId")
                    person_profile_completion_rate = profile.get("completionRate")

                    positions = profile.get("positions", [])
                    for position in positions:
                        if position.get("current"):
                            person_profile_position_start_date = position.get("startDate")
                            person_profile_position_end_date = position.get("endDate")

            unitCost = talent.get("unitCost", {})
            if unitCost:
                unit_cost_value = unitCost.get("value")

            unitRate = talent.get("unitRate", {})
            if unitRate:
                unit_rate_value = unitRate.get("value")

            tags = talent.get("tags", [])
            departement: str = ""
            geography: str = ""
            for tag in tags:
                if tag[0:3] == "dpt":
                    departement = tag
                elif tag[0:3] == "geo":
                    geography = tag

            cleaned_talent = {
                "id": talent.get("id"),
                "time_stamp": timestamp,
                # "subcategory": talent.get("subCategory"),
                "subcategory": departement,
                "geography": geography,
                "scope": talent.get("scope"),
                "manager_id": talent.get("managerId"),
                "person_profile_version_name": person_profile_version_name,
                "person_profile_person_id": person_profile_person_id,
                "person_profile_talent_id": person_profile_talent_id,
                "person_id": person_id,
                "main_person_email_address": main_email_address,
                "person_first_name": person_first_name,
                "person_last_name": person_last_name,
                "workspace_id": talent.get("workspaceId"),
                "person_main_workspace_id": person_main_workspace_id,
                "unit_cost_value": unit_cost_value,
                "unit_rate_value": unit_rate_value,
                "staffable": talent.get("staffable"),
                "entry_date": talent.get("entryDate"),
                "exit_date": talent.get("exitDate"),
                "role": talent.get("role"),
                "person_profile_completion_rate": person_profile_completion_rate,
                "person_profile_position_start_date": person_profile_position_start_date,
                "person_profile_position_end_date": person_profile_position_end_date,
            }
            cleaned_talents.append(cleaned_talent)
        return cleaned_talents

    def clean_tasks(tasks: list, timestamp: str) -> list:
        cleaned_tasks = []
        for task in tasks:
            # https://whoz.stoplight.io/docs/whoz-api/e168d751db277-task

            # Initialization of the elements that aren't on the first level of the dict
            initially_budgeted_workload_value = None
            metadata_first_worklog_date = None
            metadata_last_worklog_date = None
            metadata_total_workload_value = None
            unit_cost_value = None
            budgeted_workload_value = None
            unit_rate_value = None
            template_candidate_similar_to_candidate_id = None

            initiallyBudgetedWorkload = task.get("initiallyBudgetedWorkload", {})
            if initiallyBudgetedWorkload:
                initially_budgeted_workload_value = initiallyBudgetedWorkload.get("value")

            metadata = task.get("metadata", {})
            if metadata:
                metadata_first_worklog_date = metadata.get("firstWorklogDate")
                metadata_last_worklog_date = metadata.get("lastWorklogDate")
                totalWorkload = metadata.get("totalWorkload", {})
                if totalWorkload:
                    metadata_total_workload_value = totalWorkload.get("value")

            unitCost = task.get("unitCost", {})
            if unitCost:
                unit_cost_value = unitCost.get("value")

            budgetedWorkload = task.get("budgetedWorkload", {})
            if budgetedWorkload:
                budgeted_workload_value = budgetedWorkload.get("value")

            unitRate = task.get("unitRate", {})
            if unitRate:
                unit_rate_value = unitRate.get("value")

            templateCandidate = task.get("templateCandidate", {})
            if templateCandidate:
                template_candidate_similar_to_candidate_id = templateCandidate.get("similarToCandidateId")

            cleaned_task = {
                "id": task.get("id"),
                "time_stamp": timestamp,
                "assignee_id": task.get("assigneeId"),
                "last_modified_date": task.get("lastModifiedDate"),
                "initially_budgeted_workload_value": initially_budgeted_workload_value,
                "started_on": extract_date(task.get("startedOn")),
                "dossier_id": task.get("dossierId"),
                "start_date": task.get("startDate"),
                "end_date": task.get("endDate"),
                "metadata_first_worklog_date": metadata_first_worklog_date,
                "metadata_last_worklog_date": metadata_last_worklog_date,
                "metadata_total_workload_value": metadata_total_workload_value,
                "unit_cost_value": unit_cost_value,
                "budgeted_workload_value": budgeted_workload_value,
                "unit_rate_value": unit_rate_value,
                "status": task.get("status"),
                "template_candidate_similar_to_candidate_id": template_candidate_similar_to_candidate_id,
                "billable": task.get("billable"),
                "canceled_on": task.get("canceledOn"),
                "code": task.get("code"),
                "name": task.get("name"),
            }
            cleaned_tasks.append(cleaned_task)
        return cleaned_tasks

    def clean_dossiers(dossiers: list, timestamp: str) -> list:
        cleaned_dossiers = []
        for dossier in dossiers:
            # https://whoz.stoplight.io/docs/whoz-api/65fbe356c01f6-dossier

            # Initialization of the elements that aren't on the first level of the dict
            materials_costs_value = None
            amount_value = None

            materialsCosts = dossier.get("materialsCosts", {})
            if materialsCosts:
                materials_costs_value = materialsCosts.get("value")

            amount = dossier.get("amount", {})
            if amount:
                amount_value = amount.get("value")

            cleaned_dossier = {
                "id": dossier.get("id"),
                "time_stamp": timestamp,
                "subcategory": dossier.get("subCategory"),
                "workspace_id": dossier.get("workspaceId"),
                "owner_id": dossier.get("ownerId"),
                "materials_costs_value": materials_costs_value,
                "last_modified_date": dossier.get("lastModifiedDate"),
                "last_modified_by": dossier.get("lastModifiedBy"),
                "removed": dossier.get("removed"),
                "close_date": dossier.get("closeDate"),
                "stage": dossier.get("stage"),
                "sales_workflow_step": dossier.get("salesWorkflowStep"),
                "amount_value": amount_value,
                "reference": dossier.get("reference"),
                "project_id": dossier.get("projectId"),
                "end_date": dossier.get("endDate"),
                "name": dossier.get("name"),
                "billing_type": dossier.get("billingType"),
                "status": dossier.get("status"),
                "next_step": dossier.get("nextStep"),
                "account_id": dossier.get("accountId"),
                "start_date": dossier.get("startDate"),
            }
            cleaned_dossiers.append(cleaned_dossier)
        return cleaned_dossiers
