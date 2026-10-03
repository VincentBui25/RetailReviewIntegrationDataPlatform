"""Overture taxonomy and explicit, separate application category groups."""
from collections.abc import Mapping
from typing import Any


PRODUCT_CATEGORY_GROUPS = {
    **dict.fromkeys((
        "restaurant", "vietnamese_restaurant", "seafood_restaurant", "diner",
        "chicken_restaurant", "vegetarian_restaurant", "japanese_restaurant",
        "asian_restaurant", "malaysian_restaurant", "ramen_restaurant",
        "thai_restaurant", "indian_restaurant", "pizza_restaurant",
        "chinese_restaurant", "theme_restaurant", "taco_restaurant",
        "breakfast_and_brunch_restaurant", "mexican_restaurant", "sushi_restaurant",
        "soup_restaurant", "barbecue_restaurant", "bar_and_grill_restaurant",
        "korean_restaurant", "steakhouse", "sandwich_shop", "delicatessen",
    ), "restaurant"),
    **dict.fromkeys(("coffee_shop", "cafe", "internet_cafe"), "cafe_coffee"),
    **dict.fromkeys(("bakery", "cupcake_shop", "dessert_shop", "ice_cream_shop",
                     "frozen_yogurt_shop"), "bakery_dessert"),
    "fast_food_restaurant": "fast_food",
    **dict.fromkeys(("bubble_tea_shop", "tea_room", "smoothie_juice_bar"), "beverage"),
    **dict.fromkeys(("bar", "beer_bar", "pub", "beer_garden", "cocktail_bar"), "bar"),
}


def _taxonomy(place: Mapping[str, Any]) -> Mapping[str, Any]:
    value = place.get("taxonomy")
    return value if isinstance(value, Mapping) else {}


def get_primary_category(place: Mapping[str, Any]) -> str | None:
    value = _taxonomy(place).get("primary")
    return value if isinstance(value, str) and value else None


def get_category_hierarchy(place: Mapping[str, Any]) -> list[str]:
    value = _taxonomy(place).get("hierarchy")
    return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []


def is_food_place(place: Mapping[str, Any]) -> bool:
    return "food_and_drink" in get_category_hierarchy(place)


def normalize_categories(place: Mapping[str, Any]) -> list[str]:
    """Primary first, then supplied ancestors; never synthesize source categories."""
    primary = get_primary_category(place)
    supplied = ([primary] if primary else []) + get_category_hierarchy(place)
    return list(dict.fromkeys(value for value in supplied if value and value != "food_and_drink"))


def get_product_category_group(place: Mapping[str, Any]) -> str | None:
    if not is_food_place(place):
        return None
    return PRODUCT_CATEGORY_GROUPS.get(get_primary_category(place), "other_food")
