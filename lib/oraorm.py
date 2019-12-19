#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

'''
Database operation module. This module is independent with web module.
'''

import time, logging
import oradb


class Field(object):

    _count = 0

    def __init__(self, **kw):
        self.name = kw.get('name', None)
        self._default = kw.get('default', None)
        self.primary_key = kw.get('primary_key', False)
        self.nullable = kw.get('nullable', False)
        self.updatable = kw.get('updatable', True)
        self.insertable = kw.get('insertable', True)
        self.ddl = kw.get('ddl', '')
        self._order = Field._count
        Field._count = Field._count + 1

    @property
    def default(self):
        d = self._default
        return d() if callable(d) else d

    def __str__(self):
        s = ['<%s:%s,%s,default(%s),' % (self.__class__.__name__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)


class StringField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kw)


class IntegerField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = 0
        if not 'ddl' in kw:
            kw['ddl'] = 'number(10)'
        super(IntegerField, self).__init__(**kw)


class FloatField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = 0.0
        if not 'ddl' in kw:
            kw['ddl'] = 'number'
        super(FloatField, self).__init__(**kw)


class BooleanField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = False
        if not 'ddl' in kw:
            kw['ddl'] = 'number(1)'
        super(BooleanField, self).__init__(**kw)


class TextField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'varchar2(4000)'
        super(TextField, self).__init__(**kw)


class BlobField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'blob'
        super(BlobField, self).__init__(**kw)


class VersionField(Field):

    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, default=0, ddl='bigint')


_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])


def _gen_sql(table_name, mappings):
    pk = None
    sql = ['-- generating SQL for %s:' % table_name, 'create table %s (' % table_name]
    for f in sorted(mappings.values(), lambda x, y: cmp(x._order, y._order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' % f.name)
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            pk = f.name
        sql.append(nullable and '  %s %s,' % (f.name, ddl) or '  %s %s not null,' % (f.name, ddl))
    # sql.append('  primary key(%s)' % pk)
    sql = sql[:-1]
    sql.append(');')
    sql.append('alert table %s add constraint PK_%s primary key (%s);' % (table_name, table_name, pk))
    return '\n'.join(sql)

class ModelMetaclass(type):
    '''
    Metaclass for model objects.
    '''
    def __new__(cls, name, bases, attrs):
        # skip base Model class:
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)

        # store all subclasses info:
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        if not name in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class: %s' % name)

        logging.info('Scan ORMapping %s...' % name)
        mappings = dict()
        primary_key = None
        primary_key_attr = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                logging.info('Found mapping: %s => %s' % (k, v))
                # check duplicate primary key:
                if v.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                    if v.updatable:
                        logging.warning('NOTE: change primary key to non-updatable.')
                        v.updatable = False
                    if v.nullable:
                        logging.warning('NOTE: change primary key to non-nullable.')
                        v.nullable = False
                    primary_key = v
                    primary_key_attr = k
                mappings[k] = v
        # check exist of primary key:
        if not primary_key:
            raise TypeError('Primary key not defined in class: %s' % name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs['__table__'] = name.lower()
        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = primary_key
        attrs['__primary_key_attr__'] = primary_key_attr
        attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs[trigger] = None
        return type.__new__(cls, name, bases, attrs)

class Model(dict):
    '''
    Base class for ORM.

    >>> class User(Model):
    ...     id = IntegerField(primary_key=True)
    ...     name = StringField()
    ...     email = StringField(updatable=False)
    ...     passwd = StringField(default=lambda: '******')
    ...     last_modified = FloatField()
    ...     def pre_insert(self):
    ...         self.last_modified = time.time()
    >>> u = User(id=10190, name='Michael', email='orm@db.org')
    >>> r = u.insert()
    >>> u.email
    'orm@db.org'
    >>> u.passwd
    '******'
    >>> u.last_modified > (time.time() - 2)
    True
    >>> f = User.get(10190)
    >>> f.name
    u'Michael'
    >>> f.email
    u'orm@db.org'
    >>> f.email = 'changed@db.org'
    >>> r = f.update() # change email but email is non-updatable!
    >>> len(User.find_all())
    1
    >>> g = User.get(10190)
    >>> g.email
    u'orm@db.org'
    >>> r = g.delete()
    >>> len(db.select('select * from user where id=10190'))
    0
    >>> import json
    >>> print User().__sql__()
    -- generating SQL for user:
    create table `user` (
      `id` bigint not null,
      `name` varchar(255) not null,
      `email` varchar(255) not null,
      `passwd` varchar(255) not null,
      `last_modified` real not null,
      primary key(`id`)
    );
    '''
    __metaclass__ = ModelMetaclass
    db = None

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def get(cls, pk):
        '''
        Get by primary key.
        '''
        d = cls.db.select_one('select * from %s where %s=:pk' % (cls.__table__, cls.__primary_key__.name), {'pk': pk})
        return cls(**d) if d else None

    @classmethod
    def find_first(cls, where, d_args):
        '''
        Find by where clause and return one result. If multiple results found, 
        only the first one returned. If no result found, return None.
        '''
        d = cls.db.select_one('select * from %s %s' % (cls.__table__, where), d_args)
        return cls(**d) if d else None

    @classmethod
    def find_all(cls, *args):
        '''
        Find all and return list.
        '''
        L = cls.db.select('select * from %s' % cls.__table__)
        return [cls(**d) for d in L]

    @classmethod
    def find_by(cls, where, d_args):
        '''
        Find by where clause and return list.
        '''
        L = cls.db.select('select * from %s %s' % (cls.__table__, where), d_args)
        return [cls(**d) for d in L]

    @classmethod
    def count_all(cls):
        '''
        Find by 'select count(pk) from table' and return integer.
        '''
        return cls.db.select_int('select count(%s) from %s' % (cls.__primary_key__.name, cls.__table__))

    @classmethod
    def count_by(cls, where, d_args):
        '''
        Find by 'select count(pk) from table where ... ' and return int.
        '''
        return cls.db.select_int('select count(%s) from %s %s' % (cls.__primary_key__.name, cls.__table__, where), d_args)

    def update(self):
        self.pre_update and self.pre_update()
        L = []
        d_arg = {}
        for k, v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    d_arg[v.name] = getattr(self, k)
                else:
                    d_arg[v.name] = v.default
                    setattr(self, k, v.default)
                L.append('%s=:%s' % (v.name, v.name))
                # args.append(arg)
            # if v.primary_key:
            #     d_arg[v.name] = getattr(self, k)
        pk = self.__primary_key__.name
        d_arg[pk] = getattr(self, self.__primary_key_attr__)
        # args.append(getattr(self, pk))
        self.db.update('update %s set %s where %s=:%s' % (self.__table__, ','.join(L), pk, pk), d_arg)
        return self

    def delete(self):
        self.pre_delete and self.pre_delete()
        pk = self.__primary_key__.name
        d_arg = {self.__primary_key_attr__: getattr(self, pk)}
        # args = (getattr(self, pk), )
        self.db.update('delete from %s where %s=:%s' % (self.__table__, pk, pk), d_arg)
        return self

    def insert(self):
        self.pre_insert and self.pre_insert()
        params = {}
        for k, v in self.__mappings__.iteritems():
            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                params[v.name] = getattr(self, k)
        self.db.insert('%s' % self.__table__, params)
        return self


class Tmp_ps(Model):
    ps_id = IntegerField(ddl='NUMBER(15)', primary_key=True)
    bill_id = StringField(ddl='VARCHAR2(64)')
    ps_param = StringField(ddl='VARCHAR2(4000)')

if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    # oradb.create_engine('www-data', 'www-data', 'test')
    # oradb.update('drop table if exists user')
    # oradb.update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    # import doctest
    # doctest.testmod()

    dbConf = {'user': 'kt4', 'password': 'kt4', 'host': '10.7.5.164', 'port': 1521, 'sid': 'ngtst02', 'service_name': ''}
    ktdb = oradb.Db(dbConf)
    Tmp_ps.db = ktdb

    sql = 'select sysdate from dual'

    with oradb.connection(ktdb.db_ctx):
        sysdate = ktdb.select_one('select sysdate from dual')
        print(sysdate)

        tmp_ps = Tmp_ps.get(103258)
        print('type: %s' % type(tmp_ps))
        print(tmp_ps)
        print('ps_id: %d, bill_id: %s, ps_param: %s' % (tmp_ps.ps_id, tmp_ps.bill_id, tmp_ps.ps_param))

        # time.sleep(1)
        # with _CursorCtx(sql, ktdb.db_ctx) as curCtx:
        #     sysdate1 = curCtx.select_one(None)
        #     print('date1: %s' % sysdate1)
        #     sysdate2 = curCtx.select_one(None)
        #     print('date2: %s' % sysdate2)
        #
        # time.sleep(2)
        # with ktdb.open_cursor(sql) as curCtx:
        #     sysdate3 = curCtx.select_one(None)
        #     print('date3: %s' % sysdate3)
        #     sysdate4 = curCtx.select_one(None)
        #     print('date4: %s' % sysdate4)
        #
        # time.sleep(1)
        # with ktdb.open_curgrp() as curGrp:
        #     curGrp.get_cur('sysdate', sql)
        #     sysdate5 = curGrp.select_one('sysdate')
        #     print('date5: %s' % sysdate5)
        #     sysdate6 = curGrp.select_one('sysdate')
        #     print('date6: %s' % sysdate6)

