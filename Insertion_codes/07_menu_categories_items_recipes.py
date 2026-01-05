"""Taco Bell Menu categories, items, and recipes generation module - REAL DATA."""
from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
import random
import logging

logger = logging.getLogger(__name__)

# Real Taco Bell Categories
CATEGORIES_DATA = [
    ("CAT-01", "Tacos", "Classic and specialty tacos"),
    ("CAT-02", "Burritos", "Wrapped burritos of all sizes"),
    ("CAT-03", "Specialties", "Signature Taco Bell creations"),
    ("CAT-04", "Nachos", "Cheesy nacho varieties"),
    ("CAT-05", "Quesadillas", "Grilled quesadilla options"),
    ("CAT-06", "Sides", "Side items and add-ons"),
    ("CAT-07", "Drinks", "Beverages and fountain drinks"),
    ("CAT-08", "Desserts", "Sweet treats"),
    ("CAT-09", "Breakfast", "Morning menu items"),
    ("CAT-10", "Value Menu", "Budget-friendly options")
]

# Real Taco Bell Menu Items - EXPANDED TO 200+ ITEMS
MENU_ITEMS_DATA = [
    # ==================== TACOS (Category 1) - 50 Items ====================
    ("ITM-001", "Crunchy Taco - Beef", 1, 1.69, True, "Classic crunchy corn shell taco with beef"),
    ("ITM-002", "Crunchy Taco - Chicken", 1, 1.89, True, "Crunchy taco with shredded chicken"),
    ("ITM-003", "Crunchy Taco - Steak", 1, 2.29, True, "Crunchy taco with marinated steak"),
    ("ITM-004", "Soft Taco - Beef", 1, 1.79, True, "Soft flour tortilla taco with beef"),
    ("ITM-005", "Soft Taco - Chicken", 1, 1.99, True, "Soft taco with grilled chicken"),
    ("ITM-006", "Soft Taco - Steak", 1, 2.39, True, "Soft taco with premium steak"),
    ("ITM-007", "Doritos Locos Taco - Nacho Cheese", 1, 2.49, True, "Nacho Cheese Doritos shell with beef"),
    ("ITM-008", "Doritos Locos Taco - Cool Ranch", 1, 2.49, True, "Cool Ranch Doritos shell"),
    ("ITM-009", "Doritos Locos Taco - Fiery", 1, 2.49, True, "Fiery Doritos shell"),
    ("ITM-010", "Doritos Locos Taco Supreme - Nacho", 1, 2.99, True, "Supreme version with Nacho Cheese shell"),
    ("ITM-011", "Doritos Locos Taco Supreme - Cool Ranch", 1, 2.99, True, "Supreme with Cool Ranch shell"),
    ("ITM-012", "Crunchy Taco Supreme - Beef", 1, 2.49, True, "Crunchy taco with sour cream and tomatoes"),
    ("ITM-013", "Crunchy Taco Supreme - Chicken", 1, 2.69, True, "Chicken supreme taco"),
    ("ITM-014", "Crunchy Taco Supreme - Steak", 1, 2.99, True, "Steak supreme taco"),
    ("ITM-015", "Soft Taco Supreme - Beef", 1, 2.59, True, "Soft supreme taco with beef"),
    ("ITM-016", "Soft Taco Supreme - Chicken", 1, 2.79, True, "Chicken soft supreme"),
    ("ITM-017", "Soft Taco Supreme - Steak", 1, 3.09, True, "Steak soft supreme"),
    ("ITM-018", "Double Decker Taco", 1, 3.29, False, "Crunchy taco wrapped in soft tortilla with beans"),
    ("ITM-019", "Double Decker Taco Supreme", 1, 3.79, False, "Supreme double decker"),
    ("ITM-020", "Grilled Steak Soft Taco", 1, 3.99, True, "Marinated grilled steak taco"),
    ("ITM-021", "Grilled Chicken Soft Taco", 1, 3.49, True, "Marinated grilled chicken taco"),
    ("ITM-022", "Spicy Taco - Beef", 1, 1.99, True, "Crunchy taco with jalapeños"),
    ("ITM-023", "Spicy Taco - Chicken", 1, 2.19, True, "Spicy chicken taco"),
    ("ITM-024", "Fresco Crunchy Taco", 1, 1.69, True, "Fresco style - no cheese or sour cream"),
    ("ITM-025", "Fresco Soft Taco", 1, 1.79, True, "Fresco soft taco with pico"),
    ("ITM-026", "Shredded Chicken Soft Taco", 1, 1.99, True, "Slow-cooked shredded chicken"),
    ("ITM-027", "Seasoned Beef Soft Taco", 1, 1.79, True, "Classic seasoned beef"),
    ("ITM-028", "Black Bean Crunchy Taco", 1, 1.69, True, "Vegetarian black bean taco"),
    ("ITM-029", "Black Bean Soft Taco", 1, 1.79, True, "Soft black bean taco"),
    ("ITM-030", "Spicy Potato Soft Taco", 1, 2.19, True, "Potato with chipotle sauce"),
    ("ITM-031", "Triple Double Crunchy Taco", 1, 2.99, True, "Extra beef and cheese"),
    ("ITM-032", "Cantina Crispy Chicken Taco", 1, 3.49, True, "Premium crispy chicken"),
    ("ITM-033", "Street Taco - Chicken", 1, 2.49, True, "Authentic street-style chicken"),
    ("ITM-034", "Street Taco - Steak", 1, 2.79, True, "Authentic street-style steak"),
    ("ITM-035", "Street Taco - Carnitas", 1, 2.69, True, "Slow-cooked pork carnitas"),
    ("ITM-036", "Volcano Taco", 1, 2.99, False, "Red shell with lava sauce - Discontinued 2016"),
    ("ITM-037", "Black Jack Taco", 1, 2.49, False, "Black shell taco - Discontinued"),
    ("ITM-038", "Beefy Fritos Burrito Taco", 1, 2.29, True, "Taco with Fritos inside"),
    ("ITM-039", "Cool Ranch DLT - Chicken", 1, 2.69, True, "Cool Ranch shell with chicken"),
    ("ITM-040", "Nacho Cheese DLT - Chicken", 1, 2.69, True, "Nacho shell with chicken"),
    ("ITM-041", "XXL Grilled Stuft Taco", 1, 4.49, False, "Oversized grilled taco - Discontinued"),
    ("ITM-042", "Naked Chicken Chalupa Taco", 1, 4.29, False, "Chicken as the shell - Limited"),
    ("ITM-043", "Crunchy Taco - 3 Pack", 1, 4.99, True, "Three crunchy beef tacos"),
    ("ITM-044", "Soft Taco - 3 Pack", 1, 5.29, True, "Three soft beef tacos"),
    ("ITM-045", "Supreme Taco - 3 Pack", 1, 6.99, True, "Three supreme tacos"),
    ("ITM-046", "DLT Variety Pack - 3 Pack", 1, 6.99, True, "Three Doritos Locos tacos"),
    ("ITM-047", "Taco Party Pack - 12 Tacos", 1, 18.99, True, "12 tacos - your choice"),
    ("ITM-048", "Supreme Variety Pack - 12 Tacos", 1, 24.99, True, "12 supreme tacos variety"),
    ("ITM-049", "Soft & Crunchy Mix - 6 Pack", 1, 9.99, True, "3 soft and 3 crunchy tacos"),
    ("ITM-050", "Fiesta Taco 12 Pack", 1, 19.99, True, "Party-sized taco assortment"),
    
    # ==================== BURRITOS (Category 2) - 50 Items ====================
    ("ITM-051", "Bean Burrito", 2, 2.19, True, "Classic bean burrito"),
    ("ITM-052", "Bean and Cheese Burrito", 2, 2.49, True, "Beans with melted cheese"),
    ("ITM-053", "Burrito Supreme - Beef", 2, 4.99, True, "Loaded beef burrito"),
    ("ITM-054", "Burrito Supreme - Chicken", 2, 4.99, True, "Loaded chicken burrito"),
    ("ITM-055", "Burrito Supreme - Steak", 2, 5.49, True, "Loaded steak burrito"),
    ("ITM-056", "Burrito Supreme - Shredded Chicken", 2, 4.99, True, "Shredded chicken supreme"),
    ("ITM-057", "Beefy 5-Layer Burrito", 2, 3.99, True, "5 layers of beef, beans, and cheese"),
    ("ITM-058", "Cheesy Bean and Rice Burrito", 2, 2.49, True, "Vegetarian burrito"),
    ("ITM-059", "Chicken Enchilada Burrito", 2, 3.49, True, "Enchilada-style chicken burrito"),
    ("ITM-060", "Beef Enchilada Burrito", 2, 3.49, True, "Enchilada-style beef burrito"),
    ("ITM-061", "Quesarito - Beef", 2, 4.49, True, "Burrito wrapped in quesadilla"),
    ("ITM-062", "Quesarito - Chicken", 2, 4.49, True, "Chicken burrito in quesadilla"),
    ("ITM-063", "Quesarito - Steak", 2, 4.99, True, "Steak burrito in quesadilla"),
    ("ITM-064", "7-Layer Burrito", 2, 4.49, False, "Vegetarian 7-layer - Discontinued 2020"),
    ("ITM-065", "Beefy Crunch Burrito", 2, 3.99, False, "With Flamin' Hot Fritos - Limited"),
    ("ITM-066", "XXL Grilled Stuft Burrito - Beef", 2, 5.99, True, "Extra large beef burrito"),
    ("ITM-067", "XXL Grilled Stuft Burrito - Chicken", 2, 5.99, True, "Extra large chicken burrito"),
    ("ITM-068", "XXL Grilled Stuft Burrito - Steak", 2, 6.49, True, "Extra large steak burrito"),
    ("ITM-069", "Smothered Burrito - Beef", 2, 5.49, True, "Burrito covered in red sauce"),
    ("ITM-070", "Smothered Burrito - Chicken", 2, 5.49, True, "Chicken with red sauce"),
    ("ITM-071", "Combo Burrito", 2, 3.29, True, "Beef and bean combination"),
    ("ITM-072", "Beef Burrito", 2, 2.99, True, "Classic beef burrito"),
    ("ITM-073", "Chicken Burrito", 2, 3.19, True, "Grilled chicken burrito"),
    ("ITM-074", "Steak Burrito", 2, 3.69, True, "Premium steak burrito"),
    ("ITM-075", "Shredded Chicken Burrito", 2, 3.19, True, "Slow-cooked chicken burrito"),
    ("ITM-076", "Black Bean Burrito", 2, 2.49, True, "Vegetarian black bean"),
    ("ITM-077", "Power Menu Burrito - Chicken", 2, 5.49, True, "Protein-packed chicken burrito"),
    ("ITM-078", "Power Menu Burrito - Steak", 2, 5.99, True, "Protein-packed steak burrito"),
    ("ITM-079", "Power Menu Burrito - Veggie", 2, 4.99, True, "Vegetarian power burrito"),
    ("ITM-080", "Fresco Bean Burrito", 2, 2.19, True, "Fresco-style bean burrito"),
    ("ITM-081", "Fresco Burrito Supreme - Beef", 2, 4.99, True, "Fresco beef supreme"),
    ("ITM-082", "Fresco Burrito Supreme - Chicken", 2, 4.99, True, "Fresco chicken supreme"),
    ("ITM-083", "Chipotle Chicken Loaded Griller", 2, 2.99, False, "Mini chipotle burrito - Discontinued"),
    ("ITM-084", "Beefy Nacho Loaded Griller", 2, 2.99, False, "Mini nacho burrito - Discontinued"),
    ("ITM-085", "Loaded Potato Griller", 2, 2.49, True, "Potato-filled mini burrito"),
    ("ITM-086", "Spicy Chicken Burrito", 2, 3.49, True, "Chicken with jalapeños"),
    ("ITM-087", "Beefy Melt Burrito", 2, 2.49, True, "Beef and cheese burrito"),
    ("ITM-088", "Chipotle Ranch Grilled Chicken Burrito", 2, 2.49, True, "Grilled chicken value burrito"),
    ("ITM-089", "Double Beef Burrito", 2, 3.99, True, "Extra beef burrito"),
    ("ITM-090", "Grande Burrito - Beef", 2, 5.99, True, "Extra large beef burrito"),
    ("ITM-091", "Grande Burrito - Chicken", 2, 5.99, True, "Extra large chicken burrito"),
    ("ITM-092", "Volcano Burrito", 2, 4.49, False, "With lava sauce - Discontinued 2016"),
    ("ITM-093", "Beefy Fritos Burrito", 2, 2.29, True, "Beef burrito with Fritos"),
    ("ITM-094", "Shredded Chicken Mini Quesadilla Burrito", 2, 2.79, True, "Quesadilla-burrito hybrid"),
    ("ITM-095", "Burrito Especial - Beef", 2, 4.49, True, "Special beef burrito"),
    ("ITM-096", "Burrito Especial - Chicken", 2, 4.49, True, "Special chicken burrito"),
    ("ITM-097", "Cantina Power Burrito - Chicken", 2, 5.99, True, "Premium chicken power burrito"),
    ("ITM-098", "Cantina Power Burrito - Steak", 2, 6.49, True, "Premium steak power burrito"),
    ("ITM-099", "Breakfast Burrito", 2, 3.99, True, "Eggs, cheese, and potatoes"),
    ("ITM-100", "Cheesy Double Beef Burrito", 2, 2.99, True, "Extra beef with nacho cheese"),
    
    # ==================== SPECIALTIES (Category 3) - 40 Items ====================
    ("ITM-101", "Crunchwrap Supreme - Beef", 3, 5.49, True, "Iconic hexagonal grilled wrap with beef"),
    ("ITM-102", "Crunchwrap Supreme - Chicken", 3, 5.49, True, "Crunchwrap with grilled chicken"),
    ("ITM-103", "Crunchwrap Supreme - Steak", 3, 5.99, True, "Premium steak crunchwrap"),
    ("ITM-104", "Black Bean Crunchwrap Supreme", 3, 5.49, True, "Vegetarian crunchwrap"),
    ("ITM-105", "Spicy Crunchwrap", 3, 5.99, True, "Crunchwrap with jalapeños and spicy ranch"),
    ("ITM-106", "Cheesy Gordita Crunch - Beef", 3, 4.99, True, "Crunchy taco inside gordita with beef"),
    ("ITM-107", "Cheesy Gordita Crunch - Chicken", 3, 4.99, True, "Gordita crunch with chicken"),
    ("ITM-108", "Cheesy Gordita Crunch - Steak", 3, 5.49, True, "Premium steak gordita crunch"),
    ("ITM-109", "Chalupa Supreme - Beef", 3, 4.49, True, "Fried flatbread chalupa with beef"),
    ("ITM-110", "Chalupa Supreme - Chicken", 3, 4.49, True, "Chicken chalupa"),
    ("ITM-111", "Chalupa Supreme - Steak", 3, 4.99, True, "Steak chalupa"),
    ("ITM-112", "Chalupa Supreme - Black Bean", 3, 4.49, True, "Vegetarian chalupa"),
    ("ITM-113", "Double Chalupa Supreme", 3, 5.99, True, "Two proteins in one chalupa"),
    ("ITM-114", "Toasted Cheddar Chalupa", 3, 4.99, True, "Chalupa with toasted cheddar shell"),
    ("ITM-115", "Naked Chicken Chalupa", 3, 4.29, False, "Chicken as the shell - Limited"),
    ("ITM-116", "Mexican Pizza", 3, 5.49, True, "Two crispy tortillas with toppings"),
    ("ITM-117", "Mexican Pizza - Chicken", 3, 5.49, True, "Mexican pizza with chicken"),
    ("ITM-118", "Mexican Pizza - Steak", 3, 5.99, True, "Mexican pizza with steak"),
    ("ITM-119", "Veggie Mexican Pizza", 3, 5.49, True, "Vegetarian Mexican pizza"),
    ("ITM-120", "Enchirito - Beef", 3, 4.99, False, "Burrito covered in enchilada sauce"),
    ("ITM-121", "Enchirito - Chicken", 3, 4.99, False, "Chicken enchirito - Limited"),
    ("ITM-122", "Meximelt", 3, 2.99, False, "Taco-quesadilla hybrid - Discontinued 2019"),
    ("ITM-123", "Grande Nachos Box", 3, 6.99, True, "Oversized nacho box"),
    ("ITM-124", "Power Menu Bowl - Chicken", 3, 6.99, True, "Chicken power bowl"),
    ("ITM-125", "Power Menu Bowl - Steak", 3, 7.49, True, "Steak power bowl"),
    ("ITM-126", "Power Menu Bowl - Veggie", 3, 6.49, True, "Vegetarian power bowl"),
    ("ITM-127", "Cantina Chicken Bowl", 3, 7.99, True, "Premium chicken bowl"),
    ("ITM-128", "Cantina Steak Bowl", 3, 8.49, True, "Premium steak bowl"),
    ("ITM-129", "Fiesta Taco Salad - Beef", 3, 6.49, True, "Taco salad in edible bowl"),
    ("ITM-130", "Fiesta Taco Salad - Chicken", 3, 6.49, True, "Chicken taco salad"),
    ("ITM-131", "Grilled Stuft Nacho", 3, 4.99, False, "Folded nacho creation - Discontinued"),
    ("ITM-132", "Crunchy Wrap Slider", 3, 2.99, True, "Mini crunchwrap"),
    ("ITM-133", "Cheesy Gordita Flatbread", 3, 3.99, True, "Flatbread gordita"),
    ("ITM-134", "Triple Melt Burrito", 3, 3.99, True, "Three cheese burrito"),
    ("ITM-135", "Shredded Chicken Melt", 3, 3.49, True, "Melted chicken flatbread"),
    ("ITM-136", "Steak Melt", 3, 3.99, True, "Premium steak melt"),
    ("ITM-137", "Bell Beefer", 3, 2.99, False, "Taco on hamburger bun - Discontinued 1986"),
    ("ITM-138", "Pizzazz Pizza", 3, 3.99, False, "Taco Bell pizza - Discontinued 2020"),
    ("ITM-139", "Taco Salad", 3, 5.99, True, "Classic taco salad"),
    ("ITM-140", "Grande Scrambler Bowl", 3, 5.99, True, "Breakfast scramble bowl"),
    
    # ==================== NACHOS (Category 4) - 20 Items ====================
    ("ITM-141", "Nachos BellGrande", 4, 5.49, True, "Large nachos with all toppings"),
    ("ITM-142", "Nachos Supreme", 4, 4.49, True, "Nachos with supreme toppings"),
    ("ITM-143", "Chips and Nacho Cheese Sauce", 4, 2.49, True, "Tortilla chips with cheese"),
    ("ITM-144", "Chips and Salsa", 4, 2.29, True, "Chips with pico de gallo"),
    ("ITM-145", "Chips and Guacamole", 4, 3.49, True, "Chips with fresh guacamole"),
    ("ITM-146", "Triple Layer Nachos", 4, 2.99, True, "Beans, cheese, and red sauce"),
    ("ITM-147", "Loaded Beef Nachos", 4, 3.49, True, "Value nachos with beef"),
    ("ITM-148", "Loaded Chicken Nachos", 4, 3.69, True, "Value nachos with chicken"),
    ("ITM-149", "Nacho Fries", 4, 2.99, True, "Seasoned fries with nacho cheese"),
    ("ITM-150", "Nacho Fries Supreme", 4, 4.49, True, "Loaded nacho fries"),
    ("ITM-151", "Nacho Fries BellGrande", 4, 5.49, True, "BellGrande style fries"),
    ("ITM-152", "Party Pack Nachos", 4, 12.99, True, "Family-sized nachos"),
    ("ITM-153", "Grande Nachos - Beef", 4, 6.99, True, "Extra large beef nachos"),
    ("ITM-154", "Grande Nachos - Chicken", 4, 6.99, True, "Extra large chicken nachos"),
    ("ITM-155", "Grande Nachos - Steak", 4, 7.49, True, "Extra large steak nachos"),
    ("ITM-156", "Veggie Nachos", 4, 4.99, True, "Vegetarian nachos"),
    ("ITM-157", "Spicy Nachos Supreme", 4, 4.99, True, "Nachos with jalapeños"),
    ("ITM-158", "Loaded Nacho Taco", 4, 3.99, True, "Nacho-topped taco"),
    ("ITM-159", "Nacho Chips - Side", 4, 1.49, True, "Side of tortilla chips"),
    ("ITM-160", "Nacho Cheese Sauce - Side", 4, 0.99, True, "Extra nacho cheese"),
    
    # ==================== QUESADILLAS (Category 5) - 15 Items ====================
    ("ITM-161", "Cheese Quesadilla", 5, 4.99, True, "Three cheese blend quesadilla"),
    ("ITM-162", "Chicken Quesadilla", 5, 5.49, True, "Grilled chicken quesadilla"),
    ("ITM-163", "Steak Quesadilla", 5, 5.99, True, "Marinated steak quesadilla"),
    ("ITM-164", "Shredded Chicken Quesadilla", 5, 5.49, True, "Slow-cooked chicken quesadilla"),
    ("ITM-165", "Veggie Quesadilla", 5, 4.99, True, "Vegetarian quesadilla"),
    ("ITM-166", "Breakfast Quesadilla - Bacon", 5, 4.49, True, "Eggs, bacon, and cheese"),
    ("ITM-167", "Breakfast Quesadilla - Sausage", 5, 4.49, True, "Eggs, sausage, and cheese"),
    ("ITM-168", "Breakfast Quesadilla - Steak", 5, 4.99, True, "Eggs, steak, and cheese"),
    ("ITM-169", "Mini Chicken Quesadilla", 5, 2.49, True, "Smaller chicken quesadilla"),
    ("ITM-170", "Mini Cheese Quesadilla", 5, 1.99, True, "Smaller cheese quesadilla"),
    ("ITM-171", "Spicy Chicken Quesadilla", 5, 5.69, True, "Chicken quesadilla with jalapeños"),
    ("ITM-172", "Double Stacked Quesadilla", 5, 6.99, True, "Extra cheese and protein"),
    ("ITM-173", "Quesadilla - Carnitas", 5, 5.69, True, "Slow-cooked pork quesadilla"),
    ("ITM-174", "Grande Quesadilla - Chicken", 5, 7.99, True, "Extra large chicken quesadilla"),
    ("ITM-175", "Grande Quesadilla - Steak", 5, 8.49, True, "Extra large steak quesadilla"),
    
    # ==================== SIDES (Category 6) - 25 Items ====================
    ("ITM-176", "Cheesy Roll Up", 6, 1.49, True, "Melted cheese in flour tortilla"),
    ("ITM-177", "Cheesy Fiesta Potatoes", 6, 2.49, True, "Seasoned potato bites"),
    ("ITM-178", "Spicy Potato Soft Taco", 6, 2.19, True, "Potato taco with chipotle sauce"),
    ("ITM-179", "Black Beans - Side", 6, 1.49, True, "Seasoned black beans"),
    ("ITM-180", "Refried Beans - Side", 6, 1.49, True, "Classic refried beans"),
    ("ITM-181", "Pintos N Cheese", 6, 1.99, False, "Beans with cheese - Discontinued 2019"),
    ("ITM-182", "Mexican Rice - Side", 6, 1.49, True, "Seasoned Mexican rice"),
    ("ITM-183", "Chips and Cheese", 6, 2.49, True, "Chips with melted cheese"),
    ("ITM-184", "Side of Sour Cream", 6, 0.79, True, "Extra sour cream"),
    ("ITM-185", "Side of Guacamole", 6, 1.49, True, "Fresh guacamole"),
    ("ITM-186", "Side of Pico de Gallo", 6, 0.79, True, "Fresh salsa"),
    ("ITM-187", "Side of Jalapeños", 6, 0.59, True, "Sliced jalapeños"),
    ("ITM-188", "Red Sauce - Side", 6, 0.59, True, "Extra red sauce"),
    ("ITM-189", "3 Cheese Blend - Side", 6, 0.99, True, "Extra cheese"),
    ("ITM-190", "Seasoned Potatoes - Side", 6, 1.99, True, "Crispy potato bites"),
    ("ITM-191", "Tortilla - Side", 6, 0.49, True, "Extra flour tortilla"),
    ("ITM-192", "Taco Shell - Side", 6, 0.39, True, "Extra crunchy shell"),
    ("ITM-193", "Gordita Flatbread - Side", 6, 0.99, True, "Extra gordita bread"),
    ("ITM-194", "Chalupa Shell - Side", 6, 1.29, True, "Extra chalupa shell"),
    ("ITM-195", "Red Strips - Side", 6, 0.79, True, "Crunchy red strips"),
    ("ITM-196", "Avocado Ranch - Side", 6, 0.79, True, "Avocado ranch sauce"),
    ("ITM-197", "Chipotle Sauce - Side", 6, 0.79, True, "Chipotle sauce"),
    ("ITM-198", "Creamy Jalapeño - Side", 6, 0.79, True, "Creamy jalapeño sauce"),
    ("ITM-199", "Fire Sauce Packet", 6, 0.00, True, "Hot sauce packet - Free"),
    ("ITM-200", "Diablo Sauce Packet", 6, 0.00, True, "Extra hot sauce - Free"),
]

# Real Taco Bell Ingredients
INGREDIENTS_DATA = [
    ("ING-001", "Seasoned Ground Beef", "lb", "88% beef with signature recipe"),
    ("ING-002", "All-White Meat Chicken", "lb", "Marinated grilled chicken"),
    ("ING-003", "Marinated Grilled Steak", "lb", "Premium steak cuts"),
    ("ING-004", "Shredded Cheddar Cheese", "lb", "Real cheddar cheese"),
    ("ING-005", "Three-Cheese Blend", "lb", "Cheddar, Monterey Jack, Mozzarella"),
    ("ING-006", "Nacho Cheese Sauce", "lb", "Signature nacho cheese"),
    ("ING-007", "Iceberg Lettuce", "lb", "Shredded lettuce"),
    ("ING-008", "Diced Tomatoes", "lb", "Fresh tomatoes"),
    ("ING-009", "Sour Cream", "lb", "Reduced-fat sour cream"),
    ("ING-010", "Pico de Gallo", "lb", "Fresh salsa"),
    ("ING-011", "Refried Beans", "lb", "Pinto beans"),
    ("ING-012", "Black Beans", "lb", "Seasoned black beans"),
    ("ING-013", "Seasoned Rice", "lb", "Mexican-style rice"),
    ("ING-014", "Flour Tortilla (6-inch)", "each", "Soft taco tortilla"),
    ("ING-015", "Flour Tortilla (10-inch)", "each", "Burrito tortilla"),
    ("ING-016", "Flour Tortilla (12-inch)", "each", "Large burrito tortilla"),
    ("ING-017", "Corn Taco Shell", "each", "Crunchy taco shell"),
    ("ING-018", "Doritos Nacho Cheese Shell", "each", "Doritos Locos shell"),
    ("ING-019", "Doritos Cool Ranch Shell", "each", "Cool Ranch shell"),
    ("ING-020", "Doritos Fiery Shell", "each", "Fiery Doritos shell"),
    ("ING-021", "Chalupa Shell", "each", "Fried flatbread"),
    ("ING-022", "Gordita Flatbread", "each", "Soft flatbread"),
    ("ING-023", "Tostada Shell", "each", "Crispy flat shell"),
    ("ING-024", "Nacho Chips", "oz", "Tortilla chips"),
    ("ING-025", "Red Sauce", "oz", "Enchilada-style sauce"),
    ("ING-026", "Chipotle Sauce", "oz", "Spicy chipotle sauce"),
    ("ING-027", "Creamy Jalapeño Sauce", "oz", "Creamy jalapeño"),
    ("ING-028", "Avocado Ranch Sauce", "oz", "Ranch with avocado"),
    ("ING-029", "Spicy Ranch", "oz", "Spicy ranch dressing"),
    ("ING-030", "Fire Sauce", "packet", "Hot sauce packet"),
    ("ING-031", "Jalapeños", "oz", "Sliced jalapeños"),
    ("ING-032", "Guacamole", "oz", "Avocado guacamole"),
    ("ING-033", "Red Strips", "oz", "Crunchy red strips"),
    ("ING-034", "Fritos", "oz", "Corn chips"),
    ("ING-035", "Seasoned Potatoes", "oz", "Crispy potato bites"),
    ("ING-036", "Hash Browns", "each", "Breakfast hash brown"),
    ("ING-037", "Scrambled Eggs", "oz", "Breakfast eggs"),
    ("ING-038", "Sausage", "oz", "Breakfast sausage"),
    ("ING-039", "Bacon", "oz", "Breakfast bacon"),
    ("ING-040", "Cinnamon Sugar", "oz", "Dessert topping"),
    ("ING-041", "Cream Cheese Icing", "oz", "Cinnabon icing"),
]


def create_menu_dataframes(spark, seed=42):
    """Create all menu-related DataFrames with real Taco Bell data."""
    if seed:
        random.seed(seed)
    
    # Categories
    cat_df = spark.createDataFrame(
        [(i[0], i[1], i[2]) for i in CATEGORIES_DATA], 
        ["CategoryCode", "CategoryName", "Description"]
    )
    cat_df = (cat_df
        .withColumn("CategoryID", F.monotonically_increasing_id() + 1)
        .withColumn("IsActive", F.lit(1))
        .withColumn("CreatedDate", F.current_timestamp())
        .select("CategoryID", "CategoryCode", "CategoryName", "Description", "IsActive", "CreatedDate")
    )
    
    # Menu Items
    items_data = []
    for item in MENU_ITEMS_DATA:
        code, name, cat_id, price, is_active, desc = item
        items_data.append((code, name, cat_id, price, 1 if is_active else 0, desc))
    
    items_df = spark.createDataFrame(
        items_data,
        ["ItemCode", "ItemName", "CategoryID", "BasePrice", "IsActive", "Description"]
    )
    items_df = (items_df
        .withColumn("ItemID", F.monotonically_increasing_id() + 1)
        .withColumn("LaunchDateID", F.lit(10000))  # Default date
        .withColumn("CreatedDate", F.current_timestamp())
        .select("ItemID", "ItemCode", "ItemName", "CategoryID", "BasePrice",
               "IsActive", "LaunchDateID", "Description", "CreatedDate")
    )
    
    # Ingredients
    ing_df = spark.createDataFrame(
        [(i[0], i[1], i[2], i[3]) for i in INGREDIENTS_DATA],
        ["IngredientCode", "IngredientName", "UnitOfMeasure", "Description"]
    )
    ing_df = (ing_df
        .withColumn("IngredientID", F.monotonically_increasing_id() + 1)
        .withColumn("CreatedDate", F.current_timestamp())
        .select("IngredientID", "IngredientCode", "IngredientName", 
               "UnitOfMeasure", "Description", "CreatedDate")
    )
    
    # Create realistic recipes based on actual Taco Bell items
    recipes = create_realistic_recipes()
    
    rec_df = spark.createDataFrame(recipes, ["ItemID", "IngredientID", "Quantity"])
    rec_df = (rec_df
        .withColumn("RecipeItemID", F.monotonically_increasing_id() + 1)
        .withColumn("CreatedDate", F.current_timestamp())
        .select("RecipeItemID", "ItemID", "IngredientID", "Quantity", "CreatedDate")
    )
    
    return cat_df, items_df, ing_df, rec_df


def create_realistic_recipes():
    """Create realistic recipes for ALL 200+ Taco Bell menu items."""
    recipes = []
    
    # Base recipe templates by item type
    taco_base_beef = [(1, 0.125), (4, 0.050), (7, 0.025)]  # Beef, Cheese, Lettuce
    taco_base_chicken = [(2, 0.125), (4, 0.050), (7, 0.025)]  # Chicken, Cheese, Lettuce
    taco_base_steak = [(3, 0.125), (4, 0.050), (7, 0.025)]  # Steak, Cheese, Lettuce
    taco_supreme = [(8, 0.050), (9, 0.030)]  # Tomatoes, Sour Cream
    
    burrito_base = [(11, 0.150), (4, 0.075), (13, 0.100), (15, 1.0)]  # Beans, Cheese, Rice, Tortilla
    burrito_5layer = [(1, 0.125), (11, 0.125), (6, 0.100), (9, 0.050), (13, 0.100), (15, 1.0)]
    
    quesadilla_base = [(5, 0.150), (27, 0.075), (15, 1.0)]  # 3-Cheese, Jalapeño Sauce, Tortilla
    
    # Generate recipes for all 200 items systematically
    for item_id in range(1, 201):
        item_recipes = []
        
        # TACOS (1-50)
        if 1 <= item_id <= 50:
            if "Beef" in MENU_ITEMS_DATA[item_id-1][1] or "Crunchy Taco" == MENU_ITEMS_DATA[item_id-1][1][:12]:
                item_recipes.extend([(item_id, ing, qty) for ing, qty in taco_base_beef])
            elif "Chicken" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([(item_id, ing, qty) for ing, qty in taco_base_chicken])
            elif "Steak" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([(item_id, ing, qty) for ing, qty in taco_base_steak])
            elif "Black Bean" in MENU_ITEMS_DATA[item_id-1][1] or "Potato" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([(item_id, 12, 0.150), (item_id, 4, 0.050), (item_id, 7, 0.025)])
            
            # Add shell
            if "Doritos" in MENU_ITEMS_DATA[item_id-1][1]:
                if "Nacho" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 18, 1.0))
                elif "Cool Ranch" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 19, 1.0))
                elif "Fiery" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 20, 1.0))
            elif "Soft" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 14, 1.0))
            else:
                item_recipes.append((item_id, 17, 1.0))
            
            # Add supreme toppings
            if "Supreme" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([(item_id, ing, qty) for ing, qty in taco_supreme])
            
            # Special tacos
            if "Double Decker" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 11, 0.100))  # Beans
                item_recipes.append((item_id, 14, 1.0))  # Extra tortilla
        
        # BURRITOS (51-100)
        elif 51 <= item_id <= 100:
            # Base burrito ingredients
            if "Bean" in MENU_ITEMS_DATA[item_id-1][1] and "Beef" not in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([(item_id, ing, qty) for ing, qty in burrito_base])
            elif "5-Layer" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([(item_id, ing, qty) for ing, qty in burrito_5layer])
            else:
                # Protein
                if "Beef" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 1, 0.150))
                elif "Chicken" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 2, 0.150))
                elif "Steak" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 3, 0.150))
                
                # Standard burrito components
                item_recipes.extend([
                    (item_id, 11, 0.100),  # Beans
                    (item_id, 13, 0.100),  # Rice
                    (item_id, 4, 0.075),   # Cheese
                    (item_id, 7, 0.025),   # Lettuce
                    (item_id, 15, 1.0)     # Tortilla
                ])
                
                if "Supreme" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.extend([
                        (item_id, 8, 0.050),   # Tomatoes
                        (item_id, 9, 0.050)    # Sour Cream
                    ])
                
                if "Quesarito" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 5, 0.100))  # Extra cheese for quesadilla wrap
                
                if "Enchilada" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 25, 0.100))  # Red sauce
                
                if "XXL" in MENU_ITEMS_DATA[item_id-1][1] or "Grande" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 16, 1.0))  # Larger tortilla
                    # Double the protein
                    for i, rec in enumerate(item_recipes):
                        if rec[1] in [1, 2, 3]:  # Protein ingredients
                            item_recipes[i] = (rec[0], rec[1], rec[2] * 1.5)
        
        # SPECIALTIES (101-140)
        elif 101 <= item_id <= 140:
            if "Crunchwrap" in MENU_ITEMS_DATA[item_id-1][1]:
                # Protein
                if "Chicken" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 2, 0.150))
                elif "Steak" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 3, 0.150))
                elif "Black Bean" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 12, 0.150))
                else:
                    item_recipes.append((item_id, 1, 0.150))
                
                item_recipes.extend([
                    (item_id, 6, 0.100),   # Nacho Cheese
                    (item_id, 7, 0.030),   # Lettuce
                    (item_id, 8, 0.050),   # Tomatoes
                    (item_id, 9, 0.050),   # Sour Cream
                    (item_id, 23, 1.0),    # Tostada Shell
                    (item_id, 16, 1.0)     # Large Tortilla
                ])
            
            elif "Gordita" in MENU_ITEMS_DATA[item_id-1][1]:
                # Protein
                if "Chicken" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 2, 0.125))
                elif "Steak" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 3, 0.125))
                else:
                    item_recipes.append((item_id, 1, 0.125))
                
                item_recipes.extend([
                    (item_id, 5, 0.075),   # Three-Cheese
                    (item_id, 7, 0.025),   # Lettuce
                    (item_id, 29, 0.050),  # Spicy Ranch
                    (item_id, 17, 1.0),    # Corn Shell
                    (item_id, 22, 1.0)     # Gordita Flatbread
                ])
            
            elif "Chalupa" in MENU_ITEMS_DATA[item_id-1][1]:
                # Protein
                if "Chicken" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 2, 0.125))
                elif "Steak" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 3, 0.125))
                elif "Black Bean" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 12, 0.125))
                else:
                    item_recipes.append((item_id, 1, 0.125))
                
                item_recipes.extend([
                    (item_id, 4, 0.050),   # Cheddar Cheese
                    (item_id, 7, 0.025),   # Lettuce
                    (item_id, 8, 0.050),   # Tomatoes
                    (item_id, 9, 0.030),   # Sour Cream
                    (item_id, 21, 1.0)     # Chalupa Shell
                ])
            
            elif "Mexican Pizza" in MENU_ITEMS_DATA[item_id-1][1]:
                # Protein
                if "Chicken" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 2, 0.150))
                elif "Steak" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 3, 0.150))
                elif "Veggie" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 12, 0.150))
                else:
                    item_recipes.append((item_id, 1, 0.150))
                
                item_recipes.extend([
                    (item_id, 11, 0.125),  # Refried Beans
                    (item_id, 25, 0.100),  # Red Sauce
                    (item_id, 5, 0.100),   # Three-Cheese
                    (item_id, 8, 0.050),   # Tomatoes
                    (item_id, 23, 2.0)     # Two Tostada Shells
                ])
            
            elif "Bowl" in MENU_ITEMS_DATA[item_id-1][1]:
                # Protein
                if "Chicken" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 2, 0.200))
                elif "Steak" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 3, 0.200))
                elif "Veggie" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 12, 0.200))
                
                item_recipes.extend([
                    (item_id, 13, 0.200),  # Rice
                    (item_id, 12, 0.100),  # Black Beans
                    (item_id, 7, 0.050),   # Lettuce
                    (item_id, 8, 0.050),   # Tomatoes
                    (item_id, 4, 0.075),   # Cheese
                    (item_id, 9, 0.050),   # Sour Cream
                    (item_id, 32, 0.050)   # Guacamole
                ])
            
            else:
                # Generic specialty item
                item_recipes.extend([
                    (item_id, 1, 0.125),
                    (item_id, 4, 0.075),
                    (item_id, 7, 0.025),
                    (item_id, 14, 1.0)
                ])
        
        # NACHOS (141-160)
        elif 141 <= item_id <= 160:
            if "Fries" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([
                    (item_id, 35, 4.0),    # Seasoned Potatoes/Fries
                    (item_id, 6, 0.150)    # Nacho Cheese
                ])
                if "Supreme" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.extend([
                        (item_id, 1, 0.125),
                        (item_id, 8, 0.050),
                        (item_id, 9, 0.050)
                    ])
            else:
                item_recipes.extend([
                    (item_id, 24, 4.0),    # Nacho Chips
                    (item_id, 6, 0.150)    # Nacho Cheese
                ])
                
                if "BellGrande" in MENU_ITEMS_DATA[item_id-1][1] or "Supreme" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.extend([
                        (item_id, 1, 0.150),   # Beef
                        (item_id, 11, 0.125),  # Beans
                        (item_id, 9, 0.050),   # Sour Cream
                        (item_id, 8, 0.050)    # Tomatoes
                    ])
                elif "Loaded" in MENU_ITEMS_DATA[item_id-1][1]:
                    if "Chicken" in MENU_ITEMS_DATA[item_id-1][1]:
                        item_recipes.append((item_id, 2, 0.125))
                    else:
                        item_recipes.append((item_id, 1, 0.125))
                
                if "Salsa" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 10, 0.100))  # Pico de Gallo
                if "Guacamole" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 32, 0.100))  # Guacamole
        
        # QUESADILLAS (161-175)
        elif 161 <= item_id <= 175:
            if "Chicken" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 2, 0.150))
            elif "Steak" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 3, 0.150))
            elif "Carnitas" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 1, 0.150))  # Use beef as substitute
            
            item_recipes.extend([
                (item_id, 5, 0.150),   # Three-Cheese Blend
                (item_id, 27, 0.075),  # Creamy Jalapeño Sauce
                (item_id, 15, 1.0)     # Flour Tortilla
            ])
            
            if "Breakfast" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([
                    (item_id, 37, 0.100),  # Eggs
                    (item_id, 38, 0.075)   # Sausage or Bacon
                ])
            
            if "Mini" in MENU_ITEMS_DATA[item_id-1][1]:
                # Reduce quantities for mini
                item_recipes = [(rec[0], rec[1], rec[2] * 0.6) for rec in item_recipes]
            
            if "Grande" in MENU_ITEMS_DATA[item_id-1][1]:
                # Increase quantities for grande
                item_recipes = [(rec[0], rec[1], rec[2] * 1.5) for rec in item_recipes]
        
        # SIDES (176-200)
        elif 176 <= item_id <= 200:
            if "Potato" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([
                    (item_id, 35, 0.200),  # Seasoned Potatoes
                    (item_id, 6, 0.075),   # Nacho Cheese
                    (item_id, 9, 0.050)    # Sour Cream
                ])
            elif "Bean" in MENU_ITEMS_DATA[item_id-1][1]:
                if "Black" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 12, 0.200))
                else:
                    item_recipes.append((item_id, 11, 0.200))
                if "Cheese" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 6, 0.100))
            elif "Rice" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 13, 0.200))
            elif "Roll Up" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.extend([
                    (item_id, 5, 0.100),   # Three-Cheese
                    (item_id, 14, 1.0)     # Tortilla
                ])
            elif "Chips" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 24, 2.0))
                if "Cheese" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 6, 0.150))
                elif "Salsa" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 10, 0.100))
                elif "Guacamole" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 32, 0.100))
            elif "Sauce" in MENU_ITEMS_DATA[item_id-1][1]:
                # Sauce sides - just the sauce ingredient
                if "Nacho Cheese" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 6, 0.100))
                elif "Red Sauce" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 25, 0.100))
                elif "Chipotle" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 26, 0.100))
                elif "Jalapeño" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 27, 0.100))
                elif "Ranch" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 28, 0.100))
            elif "Sour Cream" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 9, 0.100))
            elif "Guacamole" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 32, 0.100))
            elif "Pico" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 10, 0.100))
            elif "Jalapeño" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 31, 0.050))
            elif "Cheese" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 5, 0.100))
            elif "Tortilla" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 14, 1.0))
            elif "Shell" in MENU_ITEMS_DATA[item_id-1][1]:
                if "Chalupa" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 21, 1.0))
                elif "Gordita" in MENU_ITEMS_DATA[item_id-1][1]:
                    item_recipes.append((item_id, 22, 1.0))
                else:
                    item_recipes.append((item_id, 17, 1.0))
            elif "Strips" in MENU_ITEMS_DATA[item_id-1][1]:
                item_recipes.append((item_id, 33, 0.050))
        
        # Add generated recipes to main list
        recipes.extend(item_recipes)
        
        # If no specific recipe was created, add a generic one
        if len(item_recipes) == 0:
            # Generic recipe with 3-5 ingredients
            num_ingredients = random.randint(3, 5)
            ingredient_ids = random.sample(range(1, 42), num_ingredients)
            for ing_id in ingredient_ids:
                qty = round(random.uniform(0.025, 0.150), 3)
                recipes.append((item_id, ing_id, qty))
    
    return recipes


def main():
    """Generate Taco Bell menu tables."""
    spark = get_spark("TacoBell-Menu")
    configure_azure_blob_storage(spark)
    
    try:
        logger.info("Generating Taco Bell menu with real items...")
        cat_df, items_df, ing_df, rec_df = create_menu_dataframes(spark)
        
        # Display sample data
        print("\n=== CATEGORIES ===")
        cat_df.show(10, truncate=False)
        
        print("\n=== MENU ITEMS (Sample) ===")
        items_df.show(10, truncate=False)
        
        print("\n=== INGREDIENTS (Sample) ===")
        ing_df.show(10, truncate=False)
        
        print("\n=== RECIPES (Sample) ===")
        rec_df.show(10, truncate=False)
        
        # Write to Azure Blob
        azure_menu_path = get_azure_blob_path("menu")
        write_parquet(cat_df, f"{azure_menu_path}/categories")
        write_parquet(items_df, f"{azure_menu_path}/items")
        write_parquet(ing_df, f"{azure_menu_path}/ingredients")
        write_parquet(rec_df, f"{azure_menu_path}/recipes")
        
        logger.info("✅ Taco Bell menu tables created successfully!")
        print(f"\n✅ Files written to: {azure_menu_path}")
        print(f"   - Categories: {cat_df.count()} records")
        print(f"   - Items: {items_df.count()} records")
        print(f"   - Ingredients: {ing_df.count()} records")
        print(f"   - Recipes: {rec_df.count()} records")
        
    except Exception as e:
        logger.error(f"❌ Error generating menu: {str(e)}")
        raise


if __name__ == "__main__":
    main()