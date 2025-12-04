import logging


def get_months_list(date: str, n: int) -> list[str]:
    """Generate a list of the current month and the n following months"""
    month_list = [date]
    year, month = map(int, date.split("-"))

    for _ in range(n):
        month += 1

        if month > 12:
            month = 1
            year += 1

        date = str(year) + "-" + str(month).zfill(2)
        month_list.append(date)

    return month_list


def is_empty(x):
    return x is None or x == {} or x == []


def remove_empty_elements(d: dict) -> dict:
    """Recursively remove empty lists, empty dicts, or None elements from a dictionary"""

    if not isinstance(d, (dict, list)):
        return d
    elif isinstance(d, list):
        return [v for v in (remove_empty_elements(v) for v in d) if not is_empty(v)]
    else:
        return {k: v for k, v in ((k, remove_empty_elements(v)) for k, v in d.items()) if not is_empty(v)}


def remove_structure_from_talents(talents: list) -> list:
    cleaned_talents = []
    for talent in talents:
        talent_subCategory_name = talent.get("subCategory", {})

        if talent_subCategory_name:

            if "structure" not in talent_subCategory_name.lower():
                cleaned_talents.append(talent)
            else:
                f_name = talent.get("person", {}).get("firstName", "")
                l_name = talent.get("person", {}).get("lastName", "")
                logging.info(f"{f_name} {l_name} was excluded because they were identified as STRUCTURE")
        else:
            cleaned_talents.append(talent)

    return cleaned_talents

# def remove_structure_from_talents(talents: list) -> list:
#     cleaned_talents = []
#     for talent in talents:
#         talent_version_profile_version_name: str = (
#             talent.get("person", {}).get("profile", {}).get("versionName", "").lower()
#         )
#         if "structure" not in talent_version_profile_version_name:
#             cleaned_talents.append(talent)
#         else:
#             f_name = talent.get("person", {}).get("firstName", "")
#             l_name = talent.get("person", {}).get("lastName", "")
#             logging.info(f"{f_name} {l_name} was excluded because they were identified as STRUCTURE")
#     return cleaned_talents


def extract_date(date_or_timestamp: str) -> str:
    if date_or_timestamp:
        return date_or_timestamp[:10]
    else:
        return None
