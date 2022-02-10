# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: ydb/public/api/grpc/ydb_scheme_v1.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from ydb.public.api.protos import ydb_scheme_pb2 as ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='ydb/public/api/grpc/ydb_scheme_v1.proto',
  package='Ydb.Scheme.V1',
  syntax='proto3',
  serialized_options=b'\n\030com.yandex.ydb.scheme.v1',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\'ydb/public/api/grpc/ydb_scheme_v1.proto\x12\rYdb.Scheme.V1\x1a&ydb/public/api/protos/ydb_scheme.proto2\xcc\x03\n\rSchemeService\x12T\n\rMakeDirectory\x12 .Ydb.Scheme.MakeDirectoryRequest\x1a!.Ydb.Scheme.MakeDirectoryResponse\x12Z\n\x0fRemoveDirectory\x12\".Ydb.Scheme.RemoveDirectoryRequest\x1a#.Ydb.Scheme.RemoveDirectoryResponse\x12T\n\rListDirectory\x12 .Ydb.Scheme.ListDirectoryRequest\x1a!.Ydb.Scheme.ListDirectoryResponse\x12Q\n\x0c\x44\x65scribePath\x12\x1f.Ydb.Scheme.DescribePathRequest\x1a .Ydb.Scheme.DescribePathResponse\x12`\n\x11ModifyPermissions\x12$.Ydb.Scheme.ModifyPermissionsRequest\x1a%.Ydb.Scheme.ModifyPermissionsResponseB\x1a\n\x18\x63om.yandex.ydb.scheme.v1b\x06proto3'
  ,
  dependencies=[ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2.DESCRIPTOR,])



_sym_db.RegisterFileDescriptor(DESCRIPTOR)


DESCRIPTOR._options = None

_SCHEMESERVICE = _descriptor.ServiceDescriptor(
  name='SchemeService',
  full_name='Ydb.Scheme.V1.SchemeService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=99,
  serialized_end=559,
  methods=[
  _descriptor.MethodDescriptor(
    name='MakeDirectory',
    full_name='Ydb.Scheme.V1.SchemeService.MakeDirectory',
    index=0,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._MAKEDIRECTORYREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._MAKEDIRECTORYRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='RemoveDirectory',
    full_name='Ydb.Scheme.V1.SchemeService.RemoveDirectory',
    index=1,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._REMOVEDIRECTORYREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._REMOVEDIRECTORYRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='ListDirectory',
    full_name='Ydb.Scheme.V1.SchemeService.ListDirectory',
    index=2,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._LISTDIRECTORYREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._LISTDIRECTORYRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='DescribePath',
    full_name='Ydb.Scheme.V1.SchemeService.DescribePath',
    index=3,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._DESCRIBEPATHREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._DESCRIBEPATHRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='ModifyPermissions',
    full_name='Ydb.Scheme.V1.SchemeService.ModifyPermissions',
    index=4,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._MODIFYPERMISSIONSREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__scheme__pb2._MODIFYPERMISSIONSRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_SCHEMESERVICE)

DESCRIPTOR.services_by_name['SchemeService'] = _SCHEMESERVICE

# @@protoc_insertion_point(module_scope)
