from app.utils import extract_date


class ProcessForBigQuery:
    """
    We only keep the required fields for each table
    """


    def clean_talents(talents: list, timestamp: str) -> list:
        """
        Clean the talent table
        """
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
