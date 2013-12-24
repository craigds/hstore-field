import re
import os
import psycopg2
import subprocess
from django.conf import settings
from django.db import models, DatabaseError
from django.db.backends.signals import connection_created
from psycopg2.extras import register_hstore, HstoreAdapter
from . import forms


_oids = {
    # "dbname": (hstore oid, array oid),
}

def register_hstore_on_connection_creation(connection, sender, *args, **kwargs):
    dbname = connection.alias
    if dbname not in _oids:
        oids1, oids2 = HstoreAdapter.get_oids(connection.connection)
        if not oids1 and not oids2:
            raise DatabaseError("hstore isn't installed on this database")

        _oids[dbname] = (oids1[0], oids2[0])

    oid, array_oid = _oids[dbname]
    register_hstore(connection.connection, globally=True, oid=oid, array_oid=array_oid)

connection_created.connect(register_hstore_on_connection_creation, dispatch_uid='hstore_field.register_hstore_on_connection_creation')


class HStoreDictionary(dict):

    def __init__(self, value=None, field=None, instance=None, **params):
        super(HStoreDictionary, self).__init__(value, **params)
        self.field = field
        self.instance = instance


class HStoreDescriptor(object):

    def __init__(self, field):
        self.field = field

    def __get__(self, instance=None, owner=None):
        if instance is not None:
            return instance.__dict__[self.field.name]
        else:
            raise AttributeError()

    def __set__(self, instance, value):
        if not isinstance(value, HStoreDictionary):
            value = self.field._attribute_class(value, self.field, instance)
        instance.__dict__[self.field.name] = value


class HStoreField (models.Field):

    _attribute_class = HStoreDictionary
    _descriptor_class = HStoreDescriptor

    __metaclass__ = models.SubfieldBase

    def formfield(self, **params):
        params['form_class'] = forms.HstoreField
        return super(HStoreField, self).formfield(**params)

    def contribute_to_class(self, cls, name):
        super(HStoreField, self).contribute_to_class(cls, name)
        setattr(cls, self.name, self._descriptor_class(self))

    def db_type(self, connection=None):
        return 'hstore'

    def to_python(self, value):
        if isinstance(value, dict):
            for k, v in value.iteritems():
                value[k] = forms.to_hstore(v)
        return value or {}

    def get_prep_value(self, value):
        if not value:
            return {}
        elif isinstance(value, dict):
            result = {}
            for k, v in value.iteritems():
                result[k] = forms.to_hstore(v)
            return result
        else:
            return value

    def south_field_triple(self):
        from south.modelsinspector import introspector
        field_class = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        args, kwargs = introspector(self)
        return field_class, args, kwargs
