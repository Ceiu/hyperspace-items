#!/usr/bin/python

from __future__ import print_function

import collections
from collections import OrderedDict
import ConfigParser
import inspect
import io
import json
import mysql.connector
from mysql.connector import errorcode
from optparse import OptionParser, BadOptionError
import os.path
import re
import sys


DEBUG_OUTPUT = False

LEGACY_CATEGORY_NAME = "Legacy Equipment"
LEGACY_CATEGORY_DESCRIPTION = "Legacy equipment which is no longer available"


# TODO:
# - Add transactions. We don't want to die halfway through a massive import and break stuff




# Non-whiny OptionParser, stolen from http://stackoverflow.com/a/13870300
class PassThroughOptionParser(OptionParser):
  def _process_long_opt(self, rargs, values):
    try:
      OptionParser._process_long_opt(self, rargs, values)
    except BadOptionError, err:
      self.largs.append(err.opt_str)

  def _process_short_opts(self, rargs, values):
    try:
      OptionParser._process_short_opts(self, rargs, values)
    except BadOptionError, err:
      self.largs.append(err.opt_str)




def debug(message):
  if DEBUG_OUTPUT:
    print(message)

def printerr(message):
  print(message, file=sys.stderr)

def safe_bool(input, default=False):
  try:
    return input.lower() in ['true', 't', 'yes', 'y', '1'] if isinstance(input, basestring) else bool(input)
  except ValueError:
    return default
  except TypeError:
    return default

def safe_int(input, default=0):
  try:
    return int(input)
  except ValueError:
    return default
  except TypeError:
    return default

def safe_str(input, default=None):
  try:
    return str(input) if input is not None else default
  except ValueError:
    return default
  except TypeError:
    return default

def safe_case_haskey(map, value):
  for ikey, ival in map.items():
    if (ikey.lower() == key.lower() if isinstance(ikey, basestring) and isinstance(key, basestring) else ikey == key):
      return True

  return False

def safe_case_get(map, key, func=None, default=None):
  for ikey, ival in map.items():
    if (ikey.lower() == key.lower() if isinstance(ikey, basestring) and isinstance(key, basestring) else ikey == key):
      return func(ival, default) if func is not None else ival

  return default

def safe_case_remove(map, key):
  for ikey, ival in map.items():
    if (ikey.lower() == key.lower() if isinstance(ikey, basestring) and isinstance(key, basestring) else ikey == key):
      del map[ikey]
      return ival

  return None

def safe_case_contains(list, value):
  for ival in list:
    if (value.lower() == ival.lower() if isinstance(ival, basestring) and isinstance(value, basestring) else value == ival):
      return True

  return False

def rcomp(source, update):
  def rcomp_impl(source, update):
    if isinstance(source, collections.Mapping):
      if not isinstance(update, collections.Mapping):
        return False

      for skey, sval in source.items():
        found = False

        for ukey, uval in update.items():
          if skey == ukey:
            found = True
            if not rcomp_impl(sval, uval):
              return False

            break

        if not found:
          return False

    elif isinstance(source, collections.Sequence) and not isinstance(source, basestring):
      if not (isinstance(update, collections.Sequence) and not isinstance(update, basestring)):
        return False

      for sval in source:
        found = False

        for uval in update:
          if rcomp_impl(sval, uval):
            found = True
            break

        if not found:
          return False

    else:
      return source == update

    return True

  return rcomp_impl(source, update) and rcomp_impl(update, source)


def standardize_types(types):
  if not isinstance(types, collections.Mapping):
    return None

  stdtypes = OrderedDict()

  for type, max in sorted(types.items()):
    type = safe_str(type)
    max = safe_int(max, None)

    if type is not None and max is not None:
      stdtypes[type] = max

  return stdtypes


def standardize_item(item):
  if not isinstance(item, collections.Mapping):
    return None

  stditem = OrderedDict()
  stditem["name"] = safe_case_get(item, "name", safe_str, None)
  stditem["short_description"] = safe_case_get(item, "short_description", safe_str, None)
  stditem["long_description"] = safe_case_get(item, "long_description", safe_str, None)
  stditem["buy_price"] = safe_case_get(item, "buy_price", safe_int, 0)
  stditem["sell_price"] = safe_case_get(item, "sell_price", safe_int, 0)
  stditem["exp_required"] = safe_case_get(item, "exp_required", safe_int, 0)
  stditem["ships_allowed"] = []
  stditem["max"] = safe_case_get(item, "max", safe_int, 0)
  stditem["delay_write"] = int(safe_case_get(item, "delay_write", safe_bool, False))
  stditem["ammo"] = safe_case_get(item, "ammo", safe_str, None)
  stditem["needs_ammo"] = int(safe_case_get(item, "needs_ammo", safe_bool, False))
  stditem["min_ammo"] = safe_case_get(item, "min_ammo", safe_int, 0)
  stditem["affects_sets"] = int(safe_case_get(item, "affects_sets", safe_bool, False))
  stditem["resend_sets"] = int(safe_case_get(item, "resend_sets", safe_bool, False))

  stditem["types"] = OrderedDict()
  stditem["properties"] = OrderedDict()
  stditem["events"] = []
  stditem["categories"] = OrderedDict()
  stditem["stores"] = []

  ships_allowed = safe_case_get(item, "ships_allowed")
  if isinstance(ships_allowed, collections.Sequence):
    for ship in ships_allowed:
      ship = safe_int(ship)
      if ship > 0 and ship < 9:
        stditem["ships_allowed"].append(ship)

    stditem["ships_allowed"].sort()
  else:
    ships_allowed = safe_int(ships_allowed)
    if ships_allowed is not None:
      # convert bit field to friendly numbers for allowed ship types
      for ship in range(0, 8):
        if ships_allowed & (1 << ship):
          stditem["ships_allowed"].append(ship + 1)

  item_types = safe_case_get(item, "types")
  if isinstance(item_types, collections.Mapping):
    for key, val in item_types.items():
      val = safe_int(val, None)
      if isinstance(key, basestring) and val is not None:
        stditem["types"][key] = val

  item_properties = safe_case_get(item, "properties")
  if isinstance(item_properties, collections.Mapping):
    for key, val in item_properties.items():
      val = safe_int(val, None) if not isinstance(val, basestring) else val
      if isinstance(key, basestring) and val is not None:
        stditem["properties"][key] = val

  item_events = safe_case_get(item, "events")
  if isinstance(item_events, collections.Sequence):
    for event in item_events:
      if isinstance(event, collections.Mapping):
        stdevent = OrderedDict()
        stdevent["event"] = safe_case_get(event, "event", safe_str)
        stdevent["action"] = safe_case_get(event, "action", safe_int)
        stdevent["data"] = safe_case_get(event, "data", safe_int)
        stdevent["message"] = safe_case_get(event, "message", safe_str)

        if stdevent["event"] is not None:
          stditem["events"].append(stdevent)

  item_categories = safe_case_get(item, "categories")
  if isinstance(item_categories, collections.Mapping):
    for key, val in item_categories.items():
      val = safe_int(val, None)
      if isinstance(key, basestring) and val is not None:
        stditem["categories"][key] = val

  item_stores = safe_case_get(item, "stores")
  if isinstance(item_stores, collections.Sequence):
    for store in item_stores:
      if isinstance(store, basestring):
        stditem["stores"].append(store)

    stditem["stores"].sort()

  return stditem



class ItemDB:
  def __init__(self, dbc, arena_id):
    self.dbc = dbc
    self.arena_id = arena_id

  def _start_transaction(self):
    pass

  def _commit_transaction(self):
    pass

  def _rollback_transaction(self):
    pass


  def help(self, method_name):
    """
    Displays documentation for the specified method
    """

    if isinstance(method_name, basestring) and hasattr(self, method_name):
      method = getattr(self, method_name)
      help(method)
    else:
      print("No such method: %s" % method_name, file=sys.stderr)


  def execute_query(self, query, params=None):
    cursor = self.dbc.cursor()
    cursor.execute(query, params)

    results = cursor.fetchall()
    cursor.close()

    return results

  def execute_update(self, statement, params=None):
    cursor = self.dbc.cursor()
    cursor.execute(statement, params)

    results = cursor.rowcount
    cursor.close()

    return results

  def execute_insert(self, statement, params=None):
    cursor = self.dbc.cursor()
    cursor.execute(statement, params)

    results = cursor.lastrowid
    cursor.close()

    return results


  def get_item_id(self, item_name, include_orphans=False):
    query = """
      SELECT i.id
      FROM hs_items i
      LEFT JOIN hs_category_items ci ON i.id = ci.item_id
      LEFT JOIN hs_categories c ON ci.category_id = c.id
      WHERE i.name = %s AND
    """

    if safe_bool(include_orphans):
      query += " (c.arena = %s OR c.arena IS NULL)"
    else:
      query += " c.arena = %s"

    for (id,) in self.execute_query(query, (safe_str(item_name), self.arena_id)):
      return id

    return None


  def get_item_ids(self, include_orphans=False):
    query = """
      SELECT i.id
      FROM hs_items i
      LEFT JOIN hs_category_items ci ON i.id = ci.item_id
      LEFT JOIN hs_categories c ON ci.category_id = c.id
      WHERE c.arena = %s
    """

    if safe_bool(include_orphans):
      query += " OR c.arena IS NULL"

    item_ids = []

    for (id,) in self.execute_query(query, (self.arena_id,)):
      item_ids.append(id)

    item_ids.sort()

    return item_ids

  def get_type_id(self, type):
    for (id,) in self.execute_query("SELECT id FROM hs_item_types WHERE name = %s", (safe_str(type),)):
      return id

    return None

  def create_type(self, type, max=0):
    type = safe_str(type)
    max = safe_int(max, 0)
    result = False

    if type is not None:
      if len(self.execute_query("SELECT id FROM hs_item_types WHERE name = %s", (type,))) == 0:
        result = bool(self.execute_update("INSERT INTO hs_item_types(name, max) VALUES(%s, %s)", (type, max)))
      else:
        result = bool(self.execute_update("UPDATE hs_item_types SET max = %s WHERE name = %s", (max, type)))

    return result

  def delete_type(self, type_id):
    if isinstance(type_id, basestring) and not type_id.isdigit():
      type_id = self.get_type_id(type_id)
    else:
      type_id = safe_str(type_id)

    result = False

    if type_id is not None:
      result = bool(self.execute_update("DELETE FROM hs_item_types WHERE id = %s", (type_id,)))
      if result:
        self.execute_update("DELETE FROM hs_item_type_assoc WHERE type_id = %s", (type_id,))

    return result

  def get_types(self):
    types = {}
    for (type, max) in self.execute_query("SELECT name, max FROM hs_item_types"):
      types[type] = max

    return OrderedDict(sorted(types.items()))

  def get_item_types(self, item_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    query = """
      SELECT t.name, it.qty
      FROM hs_item_types t
      INNER JOIN hs_item_type_assoc it ON it.type_id = t.id
      WHERE it.item_id = %s
    """

    types = {}
    for (type, quantity) in self.execute_query(query, (item_id,)):
      types[type] = quantity

    return OrderedDict(sorted(types.items()))

  def add_type_to_item(self, item_id, type_id, count=1):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    if isinstance(type_id, basestring) and not type_id.isdigit():
      type_id = self.get_type_id(type_id)

    count = safe_int(count, 1)
    result = False

    if item_id is not None and type_id is not None:
      if len(self.execute_query("SELECT qty FROM hs_item_type_assoc WHERE item_id = %s AND type_id = %s", (item_id, type_id))) > 0:
        result = bool(self.execute_update(
          "UPDATE hs_item_type_assoc SET qty = %s WHERE item_id = %s AND type_id = %s",
          (count, item_id, type_id)
        ))
      else:
        result = bool(self.execute_update(
          "INSERT INTO hs_item_type_assoc(item_id, type_id, qty) VALUES(%s, %s, %s)",
          (item_id, type_id, count)
        ))

    return result

  def remove_type_from_item(self, item_id, type_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    if isinstance(type_id, basestring) and not type_id.isdigit():
      type_id = self.get_type_id(type_id)

    result = False

    if item_id is not None and type_id is not None:
      result = bool(self.execute_update("DELETE FROM hs_item_type_assoc WHERE item_id = %s AND type_id = %s", (item_id, type_id)))

    return result

  def get_item_properties(self, item_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    query = """
      SELECT name, value, absolute, ignore_count
      FROM hs_item_properties WHERE item_id = %s
    """

    properties = {}
    for (name, value, absolute, ignore_count) in self.execute_query(query, (item_id,)):
      if ignore_count:
        value = "!" + str(value)

      if absolute:
        value = "=" + str(value)

      properties[name] = value

    return properties

  def add_item_property(self, item_id, property, value, absolute=False, ignore_count=False):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    property = safe_str(property)
    value = safe_str(value)
    result = False

    if item_id is not None and property is not None:
      absolute = int(safe_bool(absolute))
      ignore_count = int(safe_bool(ignore_count))

      # Process inline property updates
      while len(value) > 0:
        if value[0] == '=':
          absolute = True
          value = value[1:]
        elif value[0] == '!':
          ignore_count = True
          value = value[1:]
        else:
          break

      value = safe_int(value)

      # if the property already exists, we should update it instead
      if len(self.execute_query("SELECT value FROM hs_item_properties WHERE item_id = %s AND name = %s", (item_id, property))) > 0:
        result = bool(self.execute_update("""
          UPDATE hs_item_properties
          SET value = %s, absolute = %s, ignore_count = %s
          WHERE item_id = %s AND name = %s
        """, (value, absolute, ignore_count, item_id, property)))
      else:
        result = bool(self.execute_update(
          "INSERT INTO hs_item_properties(item_id, name, value, absolute, ignore_count) VALUES(%s, %s, %s, %s, %s)",
          (item_id, property, value, absolute, ignore_count)
        ))

    return result

  def remove_item_property(self, item_id, property):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    property = safe_str(property)

    removed = False
    if item_id is not None and property:
      removed = bool(self.execute_update("DELETE FROM hs_item_properties WHERE item_id = %s AND name = %s", (item_id, property)))

    return removed

  def get_item_events(self, item_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    query = """
      SELECT event, action, data, message
      FROM hs_item_events WHERE item_id = %s
    """

    events = []
    for (name, action, data, message) in self.execute_query(query, (item_id,)):
      event = OrderedDict()
      event["event"] = name
      event["action"] = action
      event["data"] = data
      event["message"] = message

      events.append(event)

    return events

  def add_item_event(self, item_id, event, action_id, data, message=None):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    event = safe_str(event)
    action_id = safe_int(action_id)
    data = safe_int(data)
    message = safe_str(message)
    result = False

    if item_id is not None and event:
      if len(self.execute_query("SELECT data FROM hs_item_events WHERE item_id = %s AND event = %s AND action = %s", (item_id, event, action_id))) > 0:
        result = bool(self.execute_update("""
          UPDATE hs_item_events
          SET data = %s, message = %s
          WHERE item_id = %s AND event = %s AND action = %s
        """, (data, message, item_id, event, action_id)))
      else:
        result = bool(self.execute_update(
          "INSERT INTO hs_item_events (item_id, event, action, data, message) VALUES (%s, %s, %s, %s, %s)",
          (item_id, event, action_id, data, message)
        ))

    return result

  def delete_item_event(self, item_id, event, action_id=None):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    event = safe_str(event)
    action_id = safe_int(action_id, None)
    result = False

    if item_id is not None and event:
      if action_id is not None:
        result = bool(self.execute_update("DELETE FROM hs_item_events WHERE item_id = %s AND event = %s AND action = %s", (item_id, event, action_id)))
      else:
        result = bool(self.execute_update("DELETE FROM hs_item_events WHERE item_id = %s AND event = %s", (item_id, event)))

    return result

  def get_category_id(self, category):
    for (id,) in self.execute_query("SELECT id FROM hs_categories WHERE name = %s AND arena = %s", (safe_str(category), self.arena_id)):
      return id

    return None

  def get_category_item_ids(self, category_id):
    if isinstance(category_id, basestring) and not category_id.isdigit():
      category_id = self.get_category_id(category_id)

    items = []

    for (item_id,) in self.execute_query("SELECT item_id FROM hs_category_items WHERE category_id = %s ORDER BY `order` ASC", (category_id,)):
      items.append(item_id)

    return items

  def get_category_items(self, category_id):
    if isinstance(category_id, basestring) and not category_id.isdigit():
      category_id = self.get_category_id(category_id)

    items = []

    for (item_id,) in self.execute_query("SELECT item_id FROM hs_category_items WHERE category_id = %s ORDER BY `order` ASC", (category_id,)):
      items.append(self.get_item(item_id))

    return items

  def add_category(self, category, description=None, hidden=False, order=None):
    category = safe_str(category)
    description = safe_str(description, "")
    hidden = safe_bool(hidden)

    result = False

    if category:
      current = self.execute_query("SELECT `order` FROM hs_categories WHERE arena = %s AND name = %s", (self.arena_id, category))

      if order is not None:
        order_exists = len(self.execute_query("SELECT name FROM hs_categories WHERE arena = %s AND `order` = %s", (self.arena_id, order))) > 0

        if len(current) > 0:
          # shift indexes about to deal with the repositioning
          self.execute_update("UPDATE hs_categories SET `order` = `order` - 1 WHERE arena = %s AND `order` > %s", (self.arena_id, current[0][0]))

          if order_exists:
            self.execute_update("UPDATE hs_categories SET `order` = `order` + 1 WHERE arena = %s AND `order` >= %s", (self.arena_id, order))

          result = bool(self.execute_update("UPDATE hs_categories SET `order` = %s WHERE arena = %s AND name = %s", (order, self.arena_id, category)))
        else:
          if order_exists:
            self.execute_update("UPDATE hs_categories SET `order` = `order` + 1 WHERE arena = %s AND `order` >= %s", (self.arena_id, order))

          result = bool(self.execute_update(
            "INSERT INTO hs_categories (name, description, arena, order, hidden) VALUES (%s, %s, %s, %s, %s)",
            (category, description, self.arena_id, order, int(hidden))
          ))

      else:
        if len(current) > 0:
          # impl note: this is lazy and could be improved
          self.execute_update("DELETE FROM hs_categories WHERE arena = %s AND category = %s", (self.arena_id, category))
          self.execute_update("UPDATE hs_categories SET `order` = `order` - 1 WHERE arena = %s AND `order` > %s", (self.arena_id, current[0][0]))

        # Get the highest index
        max_order = self.execute_query("SELECT MAX(`order`) FROM hs_categories WHERE arena = %s", (self.arena_id,))

        result = bool(self.execute_update(
          "INSERT INTO hs_categories (name, description, arena, `order`, hidden) VALUES (%s, %s, %s, %s, %s)",
          (category, description, self.arena_id, safe_int(max_order[0][0], -1) + 1, int(hidden))
        ))

    return result


  def get_item_categories(self, item_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    query = """
      SELECT c.name, ci.order
      FROM hs_categories c
      INNER JOIN hs_category_items ci ON ci.category_id = c.id
      WHERE ci.item_id = %s
    """

    categories = {}
    for (category, order) in self.execute_query(query, (item_id,)):
      categories[category] = order

    return OrderedDict(sorted(categories.items()))

  def add_item_to_category(self, item_id, category_id, order=None):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    if isinstance(category_id, basestring) and not category_id.isdigit():
      category_id = self.get_category_id(category_id)

    order = safe_int(order, None)
    result = False

    if item_id is not None and category_id is not None:
      current = self.execute_query("SELECT `order` FROM hs_category_items WHERE category_id = %s AND item_id = %s", (category_id, item_id))

      if order is not None:
        order_exists = len(self.execute_query("SELECT item_id FROM hs_category_items WHERE category_id = %s AND `order` = %s", (category_id, order))) > 0

        if len(current) > 0:
          # shift indexes about to deal with the repositioning
          self.execute_update("UPDATE hs_category_items SET `order` = `order` - 1 WHERE category_id = %s AND `order` > %s", (category_id, current[0][0]))

          if order_exists:
            self.execute_update("UPDATE hs_category_items SET `order` = `order` + 1 WHERE category_id = %s AND `order` >= %s", (category_id, order))

          result = bool(self.execute_update(
            "UPDATE hs_category_items SET `order` = %s WHERE category_id = %s AND item_id = %s",
            (order, category_id, item_id)
          ))
        else:
          if order_exists:
            self.execute_update("UPDATE hs_category_items SET `order` = `order` + 1 WHERE category_id = %s AND `order` >= %s", (category_id, order))

          result = bool(self.execute_update("INSERT INTO hs_category_items (category_id, item_id, `order`) VALUES (%s, %s, %s)", (category_id, item_id, order)))

      else:
        if len(current) > 0:
          # impl note: this is lazy and could be improved
          self.execute_update("DELETE FROM hs_category_items WHERE category_id = %s AND item_id = %s", (category_id, item_id))
          self.execute_update("UPDATE hs_category_items SET `order` = `order` - 1 WHERE category_id = %s AND `order` > %s", (category_id, current[0][0]))

        # Get the highest index
        max_order = self.execute_query("SELECT MAX(`order`) FROM hs_category_items WHERE category_id = %s", (category_id,))

        result = bool(self.execute_update(
          "INSERT INTO hs_category_items (category_id, item_id, `order`) VALUES (%s, %s, %s)",
          (category_id, item_id, safe_int(max_order[0][0], -1) + 1)
        ))

    return result

  def remove_item_from_category(self, item_id, category_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    if isinstance(category_id, basestring) and not category_id.isdigit():
      category_id = self.get_category_id(category_id)

    result = False

    if item_id is not None and category_id is not None:
      current = self.execute_query("SELECT `order` FROM hs_category_items WHERE category_id = %s and item_id = %s", (category_id, item_id))
      if len(current) > 0:
        self.execute_update("UPDATE hs_category_items SET `order` = `order` - 1 WHERE category_id = %s and `order` > %s", (category_id, current[0][0]))
        result = bool(self.execute_update("DELETE FROM hs_category_items WHERE category_id = %s and item_id = %s", (category_id, item_id)))

    return result


  def get_store_id(self, store):
    for (id,) in self.execute_query("SELECT id FROM hs_stores WHERE name = %s AND arena = %s", (safe_str(store), self.arena_id)):
      return id

    return None

  def get_item_stores(self, item_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    query = """
      SELECT s.name
      FROM hs_stores s
      INNER JOIN hs_store_items si ON si.store_id = s.id
      WHERE si.item_id = %s
    """

    stores = []
    for (name,) in self.execute_query(query, (item_id,)):
      stores.append(name)

    return sorted(stores)

  def add_item_to_store(self, item_id, store_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    if isinstance(store_id, basestring) and not store_id.isdigit():
      store_id = self.get_store_id(safe_str(store_id))

    result = False

    if item_id is not None and store_id is not None:
      if len(self.execute_query("SELECT store_id FROM hs_store_items WHERE store_id = %s AND item_id = %s", (store_id, item_id))) == 0:
        result = bool(self.execute_update("INSERT INTO hs_store_items (store_id, item_id) VALUES (%s, %s)", (store_id, item_id)))

    return result

  def remove_item_from_store(self, item_id, store_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    if isinstance(store_id, basestring) and not store_id.isdigit():
      store_id = self.get_store_id(safe_str(store_id))

    result = False

    if item_id is not None and store_id is not None:
      result = bool(self.execute_update("DELETE FROM hs_store_items WHERE store_id = %s AND item_id = %s", (store_id, item_id)))

    return result


  def get_item(self, item_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    query = """
      SELECT i.id, i.name, i.short_description, i.long_description, i.buy_price, i.sell_price,
        i.exp_required, i.ships_allowed, i.max, i.delay_write, i.ammo, i.needs_ammo, i.min_ammo,
        i.affects_sets, i.resend_sets
      FROM hs_items i
      INNER JOIN hs_category_items ci ON i.id = ci.item_id
      INNER JOIN hs_categories c ON ci.category_id = c.id
      WHERE c.arena = %s AND i.id = %s
    """

    item = None

    for (id, name, short_description, long_description, buy_price, sell_price, exp_required, \
      ships_allowed, max, delay_write, ammo, needs_ammo, min_ammo, affects_sets, resend_sets) \
      in self.execute_query(query, (self.arena_id, item_id)):

      # We have to create these in a specific order if we want them to output logically:
      item = OrderedDict()
      item["name"] = name
      item["short_description"] = short_description
      item["long_description"] = long_description
      item["buy_price"] = buy_price
      item["sell_price"] = sell_price
      item["exp_required"] = exp_required
      item["ships_allowed"] = []
      item["max"] = max
      item["delay_write"] = safe_bool(delay_write, False)
      item["ammo"] = None
      item["needs_ammo"] = safe_bool(needs_ammo, False)
      item["min_ammo"] = min_ammo
      item["affects_sets"] = safe_bool(affects_sets, False)
      item["resend_sets"] = safe_bool(resend_sets, False)

      # Convert ammo to an item name
      ammo_item = self.get_item(ammo)
      if ammo_item is not None:
        item["ammo"] = ammo_item["name"]

      # convert bit field to friendly numbers for allowed ship types
      for ship in range(0, 8):
        if ships_allowed & (1 << ship):
          item["ships_allowed"].append(ship + 1)

      item["types"] = self.get_item_types(item_id)
      item["properties"] = self.get_item_properties(item_id)
      item["events"] = self.get_item_events(item_id)
      item["categories"] = self.get_item_categories(item_id)
      item["stores"] = self.get_item_stores(item_id)
      break

    return item


  def get_item_player_count(self, item_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    query = """
      SELECT COUNT(DISTINCT ps.player_id)
      FROM hs_player_ships ps
      INNER JOIN hs_player_ship_items psi ON ps.id = psi.ship_id
      WHERE psi.item_id = %s
    """

    for (count,) in self.execute_query(query, (item_id,)):
      return count

    return 0


  def get_item_ship_count(self, item_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    query = """
      SELECT COUNT(ship_id)
      FROM hs_player_ship_items
      WHERE item_id = %s
    """

    for (count,) in self.execute_query(query, (item_id,)):
      return count

    return 0



  def insert_item(self, item):
    item = standardize_item(item)
    result = False

    if item is not None and item["name"] is not None:
      if self.get_item_id(item["name"]) is not None:
        return False

      self._start_transaction()

      # build ship mask:
      ships_mask = 0
      for ship in item["ships_allowed"]:
        ships_mask = ships_mask | (1 << (ship - 1))

      # Convert ammo to item id
      ammo_id = self.get_item_id(item["ammo"]) or 0

      # insert base item info
      item_id = self.execute_insert("""
        INSERT INTO hs_items(name, short_description, long_description, buy_price, sell_price,
        exp_required, ships_allowed, max, delay_write, ammo, needs_ammo, min_ammo, affects_sets,
        resend_sets) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      """, (item["name"], item["short_description"], item["long_description"], item["buy_price"],
        item["sell_price"], item["exp_required"], ships_mask, item["max"], item["delay_write"],
        ammo_id, item["needs_ammo"], item["min_ammo"], item["affects_sets"], item["resend_sets"])
      )

      if item_id is not None:
        result = True

        # add item types
        for type, count in item["types"].items():
          result = result and self.add_type_to_item(item_id, type, count)
          if not result:
            debug("failed to insert type: %s" % type)
            break

        # add item properties
        if result:
          for property, value in item["properties"].items():
            result = result and self.add_item_property(item_id, property, value)
            if not result:
              debug("failed to insert property: %s" % property)
              break

        # add item events
        if result:
          for event in item["events"]:
            result = result and self.add_item_event(item_id, event["event"], event["action"], event["data"], event["message"])
            if not result:
              debug("failed to insert event")
              break

        # add item to categories
        if result:
          for category, order in item["categories"].items():
            result = result and self.add_item_to_category(item_id, category, order)
            if not result:
              debug("failed to insert category: %s" % category)
              break

        # add item to stores
        if result:
          for store in item["stores"]:
            result = result and self.add_item_to_store(item_id, store)
            if not result:
              debug("failed to insert store: %s" % store)
              break

      if result:
        self._commit_transaction()
      else:
        self._rollback_transaction()

    return result


  def update_item(self, item_id, item):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    db_item = self.get_item(item_id)
    item = standardize_item(item)
    change_count = 0

    if db_item is not None and item is not None and item["name"] is not None:
      self._start_transaction()

      # build ship mask:
      ships_mask = 0
      for ship in item["ships_allowed"]:
        ships_mask = ships_mask | (1 << (ship - 1))

      # Convert ammo to item id
      ammo_id = self.get_item_id(item["ammo"]) or 0

      # insert base item info
      change_count += self.execute_update("""
        UPDATE hs_items SET name = %s, short_description = %s, long_description = %s, buy_price = %s,
        sell_price = %s, exp_required = %s, ships_allowed = %s, max = %s, delay_write = %s,
        ammo = %s, needs_ammo = %s, min_ammo = %s, affects_sets = %s, resend_sets = %s
        WHERE id = %s
      """, (item["name"], item["short_description"], item["long_description"], item["buy_price"],
        item["sell_price"], item["exp_required"], ships_mask, item["max"], item["delay_write"],
        ammo_id, item["needs_ammo"], item["min_ammo"], item["affects_sets"], item["resend_sets"],
        item_id)
      )

      # add item types
      for type, count in item["types"].items():
        change_count += int(self.add_type_to_item(item_id, type, count))
        safe_case_remove(db_item["types"], type)

      # remove absent types
      for type, count in db_item["types"].items():
        change_count += int(self.remove_type_from_item(item_id, type))

      # add item properties
      for property, value in item["properties"].items():
        change_count += int(self.add_item_property(item_id, property, value))
        safe_case_remove(db_item["properties"], property)

      for property, value in db_item["properties"].items():
        change_count += int(self.remove_item_property(item_id, property))

      # add item events
      # impl note: we cheat a bit here by deleting all events and recreating them. Makes maintenance
      # a bit easier.
      for event in db_item["events"]:
        change_count += int(self.delete_item_event(item_id, event["event"]))

      for event in item["events"]:
        change_count += int(self.add_item_event(item_id, event["event"], event["action"], event["data"], event["message"]))

      # add item to categories
      for category, order in item["categories"].items():
        change_count += int(self.add_item_to_category(item_id, category, order))
        safe_case_remove(db_item["categories"], category)

      for category, order in db_item["categories"].items():
        change_count += int(self.remove_item_from_category(item_id, category))

      # add item to stores
      for store in item["stores"]:
        change_count += int(self.add_item_to_store(item_id, store))

      for store in db_item["stores"]:
        if not safe_case_contains(item["stores"], store):
          change_count += int(self.remove_item_from_store(store))

      self._commit_transaction()

    return change_count > 0


  def delete_item(self, item_id):
    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    removed = False

    if item_id:
      remnants = self.execute_update("DELETE FROM hs_category_items WHERE item_id = %s", (item_id,)) + \
        self.execute_update("DELETE FROM hs_item_events WHERE item_id = %s", (item_id,)) + \
        self.execute_update("DELETE FROM hs_item_properties WHERE item_id = %s", (item_id,)) + \
        self.execute_update("DELETE FROM hs_item_type_assoc WHERE item_id = %s", (item_id,)) + \
        self.execute_update("DELETE FROM hs_player_ship_items WHERE item_id = %s", (item_id,)) + \
        self.execute_update("DELETE FROM hs_store_items WHERE item_id = %s", (item_id,))

      removed = bool(self.execute_update("DELETE FROM hs_items WHERE id = %s", (item_id,)))

    return removed

  def delete_all_items(self):
    deleted = 0

    for item_id in self.get_item_ids(True):
      if self.delete_item(item_id):
        deleted += 1

    return deleted


  def convert_to_legacy(self, item_id):
    def get_legacy_name(base_name):
      name_format = "LWM%d-%s"
      version = 1

      while True:
        legacy_name = name_format % (version, base_name)
        version += 1

        if self.get_item_id(legacy_name) is None:
          return legacy_name


    if isinstance(item_id, basestring) and not item_id.isdigit():
      item_id = self.get_item_id(item_id) or self.get_item_id(item_id, True)

    item = self.get_item(item_id)
    result = False

    if item:
      legacy_name = get_legacy_name(item["name"])
      legacy_desc = "Legacy version of the base %s" % item["name"]

      self._start_transaction()
      result = bool(self.execute_update(
        "UPDATE hs_items SET buy_price = 0, name = %s, long_description = %s WHERE id = %s",
        (legacy_name, legacy_desc, item_id)
      ))

      if result:
        result = bool(self.execute_update("DELETE FROM hs_category_items WHERE item_id = %s", (item_id,)))

      if result:
        cat_id = self.get_category_id(LEGACY_CATEGORY_NAME)
        if cat_id is None:
          if not self.add_category(LEGACY_CATEGORY_NAME, LEGACY_CATEGORY_DESCRIPTION, True):
            printerr("ERROR: Unable to create legacy item category") # uh oh
          else:
            cat_id = self.get_category_id("Legacy Equipment")

        result = cat_id is not None and self.add_item_to_category(item_id, cat_id)

      if result:
        self._commit_transaction()

    return result

  def cleanup_legacy_items(self, min_players = 1):
    cat_id = self.get_category_id(LEGACY_CATEGORY_NAME)

    removed = 0
    if cat_id is not None:
      for (item_id,) in self.execute_query("SELECT item_id FROM hs_category_items WHERE category_id = %s ORDER BY `order` ASC", (cat_id,)):
        if self.get_item_player_count(item_id) < min_players:
          self.delete_item(item_id)
          removed += 1

    return removed


  def export_items(self):
    db_data = OrderedDict()
    items = []

    for item_id in self.get_item_ids():
      items.append(self.get_item(item_id))

    db_data["types"] = self.get_types()
    db_data["items"] = items

    return db_data


  def import_items(self, make_legacy=True, destructive=False, filter=None, invert_filter=False):
    """
    Imports and updates HS items from the JSON-formatted data received on stdin.
    If make_legacy is true, any items marked for update or deletion which are in used by players
    will be retained as a legacy item; allowing players to retain their current item stats.
    If destructive is true, any item which is absent from the provided data will be converted to
    a legacy item, or deleted.
    If filter is set and is a valid regex, only items matching the expression will be imported.
    If invert_filter is true, only items not matching the expression will be imported.
    """
    make_legacy = safe_bool(make_legacy)
    destructive = safe_bool(destructive)
    invert_filter = safe_bool(invert_filter)

    item_filter = None

    try:
      if filter is not None:
        item_filter = re.compile(safe_str(filter), re.IGNORECASE)
    except re.error:
      pass


    import_data = json.load(sys.stdin)
    existing_ids = self.get_item_ids()
    updated_ids = []
    import_items = []
    import_types = None

    processed = 0
    created = 0
    updated = 0
    removed = 0
    invalid = 0
    converted = 0
    skipped = 0
    failed = 0

    # Get only the items, if we're provided a full HS JSON
    if isinstance(import_data, collections.Mapping) and "items" in import_data:
      import_items = import_data["items"]

      if "types" in import_data:
        import_types = standardize_types(import_data["types"])
    elif isinstance(import_data, collections.Sequence):
      import_items = import_data
    else:
      import_items = [import_data]


    # Sync up types if they're provided
    if isinstance(import_types, collections.Mapping):
      existing = self.get_types()
      present = []

      if not rcomp(existing, import_types):
        for type, max in import_types.items():
          present.append(type)
          self.create_type(type, max)

        for type, max in existing.items():
          if not safe_case_contains(present, type):
            self.delete_type(type)

    # TODO:
    # Add item category synchronization here. Will be a tad messier than types



    for raw_item in import_items:
      processed += 1
      # validate item, throwing it aside if it's bad
      item = standardize_item(raw_item)

      if not item or item["name"] is None:
        invalid += 1
        printerr("Invalid item found at index %d" % processed)
        continue

      db_item_id = self.get_item_id(item["name"])
      if db_item_id is not None:
        updated_ids.append(db_item_id)

      if item_filter is not None and (item_filter.match(item["name"]) if invert_filter else not item_filter.match(item["name"])):
        debug("Skipping filtered item: %s" % item["name"])
        skipped += 1
        continue

      if db_item_id is not None:
        db_item = self.get_item(db_item_id)

        # existing item; check if it changed...
        if rcomp(db_item, item):
          debug("Skipping unchanged item: %s" % item["name"])
          skipped += 1
          continue

        # check if we can just update the item directly
        if not make_legacy or self.get_item_ship_count(db_item_id) < 1:
          # Raw, in-place update on an existing item
          debug("Updating item: %s" % item["name"])
          if self.update_item(db_item_id, item):
            updated += 1
          else:
            printerr("ERROR: Unable to update existing item: %s" % item["name"])
            failed += 1

        else:
          # Convert it to a legacy item, I guess... Then just import the updated item as a new item.
          debug("Converting item to legacy item: %s" % item["name"])
          if self.convert_to_legacy(db_item_id):
            converted += 1

            debug("Inserting new version of item: %s" % item["name"])
            if self.insert_item(item):
              created += 1
            else:
              printerr("ERROR: Unable to create item: %s" % item["name"])
              failed += 1
          else:
            printerr("ERROR: Unable to convert item to legacy: %s" % item["name"])
            failed += 1

      else:
        # new item entirely; just import
        debug("Inserting item: %s" % item["name"])
        if self.insert_item(item):
          created += 1
        else:
          printerr("ERROR: Unable to create new item: %s" % item["name"])
          failed += 1


    # check if we need to remove items which were not in the update JSON
    if destructive:
      print("Removing absent items...")
      legacy_items = self.get_category_item_ids(LEGACY_CATEGORY_NAME)
      item_ids = [item_id for item_id in existing_ids if item_id not in updated_ids and item_id not in legacy_items]

      for item_id in item_ids:
        if make_legacy and self.get_item_ship_count(db_item_id) > 0:
          # convert to legacy item
          debug("Converting absent item to legacy item: %s" % self.get_item(item_id)["name"])
          self.convert_to_legacy(item_id)
          converted += 1

        else:
          # just delete it. This is dangerous while the server is running.
          debug("Deleting item: %s" % self.get_item(item_id)["name"])
          self.delete_item(item_id)
          removed += 1

    output = OrderedDict()
    output["processed"] = processed
    output["created"] = created
    output["updated"] = updated
    output["removed"] = removed
    output["invalid"] = invalid
    output["converted"] = converted
    output["skipped"] = skipped
    output["failed"] = failed

    return output

####################################################################################################

def main(argv):
  def get_db_connection(host, database, username, password):
    try:
      dbc = mysql.connector.connect(host=host, database=database, user=username, password=password)
      return dbc
    except mysql.connector.Error as e:
      if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        printerr("Invalid username and/or password")
      elif e.errno == errorcode.ER_BAD_DB_ERROR:
        printerr("Database %s does not exist" % database)
      else:
        printerr(e)

    return None

  def find_section(config, desired_sections):
    for section in config.sections():
      for desired in desired_sections:
        if desired.lower() == section.lower():
          return section

    return None

  parser = PassThroughOptionParser()
  parser.add_option("-a", "--arena", action="store", dest="arena_id", default="main")
  parser.add_option("--config", action="store", default=None)
  parser.add_option("--dbhost", action="store", default="localhost")
  parser.add_option("--database", action="store", default="hyperspace")
  parser.add_option("--dbuser", action="store", default="hyperspace")
  parser.add_option("--dbpass", action="store", default=None)
  (options, args) = parser.parse_args()

  # Setup DB connection
  if options.config:
    # We have a config file! Try to read it and parse DB info from it.
    config = ConfigParser.RawConfigParser()

    if options.config not in config.read(options.config):
      printerr("Unable to read configuration file: %s" % options.config)
      return;

    section = find_section(config, ["hyperspace", "mysql"])
    if section is None:
      printerr("Unable to find database configuration sections \"hyperspace\" or \"mysql\" in config file %s" % options.config)
      return;

    options.dbhost = config.get(section, "hostname");
    options.database = config.get(section, "database");
    options.dbuser = config.get(section, "user");
    options.dbpass = config.get(section, "password");

  dbc = get_db_connection(options.dbhost, options.database, options.dbuser, options.dbpass)
  if dbc is not None:
    item_db = ItemDB(dbc, options.arena_id)

    if len(args) > 0:
      if hasattr(item_db, args[0]) and args[0][0] != '_':
        method = getattr(item_db, args[0])
        args = args[1:]

        # Ensure arg count matches up. Extend with None or truncate as necessary
        arginfo = inspect.getargspec(method)
        argcount = len(arginfo[0]) - 1
        defcount = len(arginfo[3]) if arginfo[3] else 0

        if len(args) > argcount:
          args = args[:argcount]
        elif len(args) < argcount:
          if defcount < (argcount - len(args)):
            args += ([None] * (argcount - len(args) - defcount))

          if defcount > 0:
            offset = len(args)
            while offset < argcount:
              args.append(arginfo[3][offset - argcount])
              offset += 1

        result = method(*args)

        if result is not None:
          json.dump(result, sys.stdout, indent=2)
          print()

        item_db._rollback_transaction()

      else:
        printerr("ERROR: No such action: %s" % args[0])

    else:
      printerr("ERROR: Must specify an action to perform")

    dbc.close()

main(sys.argv)
