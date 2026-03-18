"""
Semantic node schema definitions for the OBC Knowledge Graph.

Defines 5 semantic node types:
1. Topic - Regulatory subjects (FireSafety, Stairs, Egress, etc.)
2. BuildingType - Building classifications (Residential, Industrial, etc.)
3. OccupancyClass - OBC occupancy groups (A, B, C, D, E, F)
4. ConstructionType - Construction methods (Combustible, NonCombustible, etc.)
5. SpaceType - Specific spaces (Basement, Bathroom, Stairwell, etc.)
"""

from typing import List, Dict, Any


# ==========================
# SEMANTIC NODE DEFINITIONS
# ==========================

TOPIC_SEED_DATA = [
    {"name": "OccupancyClassification", "description": "Rules for categorizing buildings based on their intended use, such as assembly, care, residential, or industrial occupancies.", "keywords": ["major occupancy", "Group A", "Group B", "Group C", "hazard index", "classification", "subsidiary occupancy"]},
    {"name": "FireSeparationsAndClosures", "description": "Requirements for physical barriers and doors designed to retard the spread of fire and smoke between compartments.", "keywords": ["fire separation", "fire-resistance rating", "firewall", "closure", "fire stop", "fire block", "fire damper"]},
    {"name": "MeansOfEgress", "description": "Regulations governing the path of travel for occupants to evacuate a building safely, including exits and access to exits.", "keywords": ["exit", "access to exit", "travel distance", "egress doorway", "public corridor", "exit sign", "dead-end corridor"]},
    {"name": "FireAlarmAndDetection", "description": "Standards for systems that detect fire or dangerous gases and alert building occupants.", "keywords": ["fire alarm system", "smoke detector", "heat detector", "carbon monoxide alarm", "annunciator", "voice communication system", "sprinkler system"]},
    {"name": "StructuralDesignAndLoads", "description": "Criteria for calculating dead, live, snow, wind, and earthquake loads to ensure structural integrity.", "keywords": ["dead load", "live load", "snow load", "seismic", "earthquake force", "deflection", "limit states design"]},
    {"name": "ExcavationAndFoundations", "description": "Requirements for earthworks, footings, and walls that support the building structure against soil and water pressure.", "keywords": ["excavation", "foundation wall", "footing", "soil bearing capacity", "backfill", "drainage tile", "dampproofing"]},
    {"name": "WoodConstruction", "description": "Standards for lumber grading, framing, spans, and connections in wood-frame buildings.", "keywords": ["lumber grade", "joist span", "stud", "lintel", "wood-frame construction", "sheathing", "blocking"]},
    {"name": "MasonryAndConcrete", "description": "Specifications for the use of brick, block, mortar, grout, and reinforced concrete in building elements.", "keywords": ["masonry unit", "mortar", "grout", "reinforced concrete", "compressive strength", "corrosion protection", "lateral support"]},
    {"name": "EnvironmentalSeparation", "description": "Measures to control the flow of heat, air, and moisture through the building envelope.", "keywords": ["thermal insulation", "vapour barrier", "air barrier", "cladding", "roofing", "flashing", "condensation control"]},
    {"name": "HVACSystems", "description": "Regulations for heating, ventilating, and air-conditioning systems to ensure indoor air quality and comfort.", "keywords": ["ventilation", "ductwork", "plenum", "exhaust fan", "combustion air", "supply air", "heating appliance"]},
    {"name": "PlumbingSystems", "description": "Standards for the design, materials, and installation of water supply and drainage systems.", "keywords": ["potable water", "sanitary drainage", "vent pipe", "fixture unit", "trap", "cleanout", "interceptors"]},
    {"name": "SewageSystems", "description": "Rules for on-site private sewage disposal, including septic tanks and leaching beds.", "keywords": ["septic tank", "leaching bed", "distribution pipe", "effluent", "holding tank", "percolation time", "treatment unit"]},
    {"name": "WindowsDoorsAndSkylights", "description": "Performance and installation requirements for glazed openings and entryways.", "keywords": ["safety glass", "skylight", "window area", "door swing", "air leakage", "water penetration", "forced entry"]},
    {"name": "StairsRampsAndGuards", "description": "Dimensions and safety features for vertical circulation elements and protective barriers.", "keywords": ["rise and run", "tread depth", "handrail", "guard height", "climbable", "ramp gradient", "landing"]},
    {"name": "Accessibility", "description": "Design standards to ensure buildings are accessible to persons with physical or sensory disabilities.", "keywords": ["barrier-free path", "power door operator", "universal washroom", "tactile attention indicator", "wheelchair space", "turning space", "clear width"]},
    {"name": "SoundTransmission", "description": "Requirements to limit noise transfer between dwelling units and other spaces.", "keywords": ["sound transmission class", "STC rating", "apparent sound transmission", "noise control", "separation of suites", "airborne sound"]},
    {"name": "PublicPoolsAndSpas", "description": "Safety and sanitary regulations for the construction and operation of pools and hot tubs.", "keywords": ["public pool", "public spa", "deck", "skimmer", "circulation system", "emergency stop", "water clarity"]},
    {"name": "FarmBuildings", "description": "Specific provisions for agricultural structures dealing with low occupant loads and high-hazard contents.", "keywords": ["agricultural occupancy", "liquid manure", "crop drying", "farm building", "low human occupancy", "greenhouse", "silo"]},
    {"name": "RoomDimensions", "description": "Minimum and maximum requirements for room height, floor area, width, and other spatial dimensions.", "keywords": ["ceiling height", "headroom", "floor area", "minimum width", "room size", "clear height", "storey height"]},
    {"name": "FireResistanceRatings", "description": "Specific hourly fire-resistance ratings required for building assemblies, columns, beams, and membranes.", "keywords": ["fire-resistance rating", "FRR", "1 h", "2 h", "45 min", "fire-rated assembly", "protected construction"]},
    {"name": "LightingAndNaturalLight", "description": "Requirements for natural light, windows, and artificial lighting in habitable and work spaces.", "keywords": ["natural light", "window area", "glass area", "lighting", "daylight", "glazed area", "illumination"]},
]

BUILDING_TYPE_SEED_DATA = [
    {"name": "Residential", "description": "Houses, apartments, and dwelling units", "obc_parts": ["9"], "keywords": ["house", "dwelling", "apartment", "home", "residential", "condo"]},
    {"name": "Industrial", "description": "Factories, warehouses, and manufacturing facilities", "obc_parts": ["3"], "keywords": ["industrial", "factory", "warehouse", "manufacturing", "plant"]},
    {"name": "Commercial", "description": "Offices, retail, and business occupancies", "obc_parts": ["3"], "keywords": ["commercial", "office", "retail", "store", "shop", "business"]},
    {"name": "Assembly", "description": "Theaters, arenas, and gathering spaces", "obc_parts": ["3"], "keywords": ["assembly", "theater", "arena", "auditorium", "gymnasium", "restaurant"]},
    {"name": "Institutional", "description": "Hospitals, schools, and care facilities", "obc_parts": ["3"], "keywords": ["institutional", "hospital", "school", "care", "detention", "treatment"]},
    {"name": "MixedUse", "description": "Buildings with multiple occupancy types", "obc_parts": ["3", "9"], "keywords": ["mixed", "mixed-use", "multiple occupancy"]},
]

OCCUPANCY_CLASS_SEED_DATA = [
    {"name": "Group A - Assembly", "code": "A", "divisions": ["A1", "A2", "A3", "A4"], "description": "Assembly occupancies for gatherings"},
    {"name": "Group B - Institutional", "code": "B", "divisions": ["B1", "B2", "B3"], "description": "Institutional occupancies including detention and care"},
    {"name": "Group C - Residential", "code": "C", "divisions": [], "description": "Residential occupancies including dwelling units"},
    {"name": "Group D - Business", "code": "D", "divisions": [], "description": "Business and personal services occupancies"},
    {"name": "Group E - Mercantile", "code": "E", "divisions": [], "description": "Mercantile occupancies including retail"},
    {"name": "Group F - Industrial", "code": "F", "divisions": ["F1", "F2", "F3"], "description": "Industrial occupancies including factories"},
]

CONSTRUCTION_TYPE_SEED_DATA = [
    {"name": "Combustible", "description": "Wood-frame and combustible construction", "keywords": ["combustible", "wood", "frame", "wood-frame"]},
    {"name": "NonCombustible", "description": "Non-combustible construction materials", "keywords": ["noncombustible", "non-combustible", "steel", "concrete", "masonry"]},
    {"name": "FireResistive", "description": "Fire-resistive rated construction", "keywords": ["fire-resistive", "fire rated", "fire resistance rating"]},
    {"name": "HeavyTimber", "description": "Heavy timber construction", "keywords": ["heavy timber", "mass timber", "glulam", "CLT"]},
]

SPACE_TYPE_SEED_DATA = [
    {"name": "Basement", "description": "A storey or storeys of a building located below the first storey.", "keywords": ["basement", "below grade", "foundation wall", "cellar", "underground"]},
    {"name": "CrawlSpace", "description": "A low-height space between the lowest floor assembly and the ground, often used for services.", "keywords": ["crawl space", "unexcavated area", "ground cover", "ventilation", "access hatch"]},
    {"name": "Attic", "description": "The space between the roof and the ceiling of the top storey.", "keywords": ["attic", "roof space", "insulation", "roof vent", "hatchway"]},
    {"name": "ServiceRoom", "description": "A room provided in a building to contain equipment associated with building services.", "keywords": ["service room", "furnace room", "boiler room", "electrical room", "mechanical room"]},
    {"name": "PublicCorridor", "description": "A corridor that provides access to exit from more than one suite.", "keywords": ["public corridor", "hallway", "common corridor", "access to exit", "fire separation"]},
    {"name": "ExitStairway", "description": "A stairway that is part of an exit facility leading directly to the outdoors.", "keywords": ["exit stair", "stair shaft", "fire escape", "enclosed stairway", "means of egress"]},
    {"name": "StorageGarage", "description": "A building or part thereof intended for the storage or parking of motor vehicles.", "keywords": ["storage garage", "parking garage", "parkade", "vehicle storage", "repair garage"]},
    {"name": "Kitchen", "description": "A room or area where food is prepared, ranging from residential to commercial cooking facilities.", "keywords": ["kitchen", "cooking equipment", "grease duct", "commercial cooking", "exhaust hood"]},
    {"name": "Bathroom", "description": "A room containing plumbing fixtures such as a water closet, lavatory, and bathtub or shower.", "keywords": ["bathroom", "washroom", "water closet room", "sanitary unit", "toilet room"]},
    {"name": "Bedroom", "description": "A room designed or intended for sleeping within a dwelling unit or care facility.", "keywords": ["bedroom", "sleeping room", "sleeping area", "dormitory", "patient room"]},
    {"name": "LivingArea", "description": "General habitable space within a dwelling used for daily activities.", "keywords": ["living room", "dining room", "recreation room", "den", "habitable room"]},
    {"name": "Mezzanine", "description": "An intermediate floor assembly between the floor and ceiling of any room or storey.", "keywords": ["mezzanine", "intermediate floor", "open floor area", "loft", "interior balcony"]},
    {"name": "Suite", "description": "A single room or series of rooms of complementary use, operated under a single tenancy.", "keywords": ["suite", "dwelling unit", "apartment", "guest room", "tenant space"]},
    {"name": "UniversalWashroom", "description": "A large, barrier-free washroom designed for use by people with disabilities and their attendants.", "keywords": ["universal washroom", "barrier-free toilet", "adult change table", "emergency call system", "transfer space"]},
    {"name": "HighHazardRoom", "description": "A room containing highly flammable or explosive materials requiring special fire separation.", "keywords": ["hazardous room", "paint spray booth", "chemical storage", "industrial process", "explosion venting"]},
    {"name": "Lobby", "description": "A vestibule or entry hall, often serving as a foyer or waiting area near an exit.", "keywords": ["lobby", "foyer", "entrance hall", "waiting area", "elevator lobby"]},
    {"name": "Vestibule", "description": "A small enclosed area between the outer door and the interior of a building to control air movement.", "keywords": ["vestibule", "air lock", "entryway", "entrance vestibule", "security vestibule"]},
    {"name": "Balcony", "description": "An exterior platform projecting from the wall of a building, or an interior gallery overlooking a floor below.", "keywords": ["balcony", "terrace", "deck", "veranda", "exterior passageway"]},
]


# ==========================
# NEO4J CONSTRAINT QUERIES
# ==========================

SEMANTIC_CONSTRAINTS = [
    "CREATE CONSTRAINT topic_name_unique IF NOT EXISTS FOR (t:Topic) REQUIRE t.name IS UNIQUE",
    "CREATE CONSTRAINT building_type_name_unique IF NOT EXISTS FOR (bt:BuildingType) REQUIRE bt.name IS UNIQUE",
    "CREATE CONSTRAINT occupancy_class_code_unique IF NOT EXISTS FOR (oc:OccupancyClass) REQUIRE oc.code IS UNIQUE",
    "CREATE CONSTRAINT construction_type_name_unique IF NOT EXISTS FOR (ct:ConstructionType) REQUIRE ct.name IS UNIQUE",
    "CREATE CONSTRAINT space_type_name_unique IF NOT EXISTS FOR (st:SpaceType) REQUIRE st.name IS UNIQUE",
]


# ==========================
# SCHEMA HELPER FUNCTIONS
# ==========================

def get_all_seed_data() -> Dict[str, List[Dict[str, Any]]]:
    """Return all seed data for semantic nodes."""
    return {
        "Topic": TOPIC_SEED_DATA,
        "BuildingType": BUILDING_TYPE_SEED_DATA,
        "OccupancyClass": OCCUPANCY_CLASS_SEED_DATA,
        "ConstructionType": CONSTRUCTION_TYPE_SEED_DATA,
        "SpaceType": SPACE_TYPE_SEED_DATA,
    }


def get_all_topic_names() -> List[str]:
    """Return all predefined topic names."""
    return [t["name"] for t in TOPIC_SEED_DATA]


def get_all_building_type_names() -> List[str]:
    """Return all predefined building type names."""
    return [bt["name"] for bt in BUILDING_TYPE_SEED_DATA]


def get_all_occupancy_codes() -> List[str]:
    """Return all occupancy class codes."""
    return [oc["code"] for oc in OCCUPANCY_CLASS_SEED_DATA]


def get_all_construction_type_names() -> List[str]:
    """Return all construction type names."""
    return [ct["name"] for ct in CONSTRUCTION_TYPE_SEED_DATA]


def get_all_space_type_names() -> List[str]:
    """Return all space type names."""
    return [st["name"] for st in SPACE_TYPE_SEED_DATA]
