# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: ydb/public/api/grpc/ydb_rate_limiter_v1.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from ydb.public.api.protos import ydb_rate_limiter_pb2 as ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='ydb/public/api/grpc/ydb_rate_limiter_v1.proto',
  package='Ydb.RateLimiter.V1',
  syntax='proto3',
  serialized_options=b'\n\036com.yandex.ydb.rate_limiter.v1B\017RateLimiterGrpcP\001',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n-ydb/public/api/grpc/ydb_rate_limiter_v1.proto\x12\x12Ydb.RateLimiter.V1\x1a,ydb/public/api/protos/ydb_rate_limiter.proto2\xe3\x04\n\x12RateLimiterService\x12\x61\n\x0e\x43reateResource\x12&.Ydb.RateLimiter.CreateResourceRequest\x1a\'.Ydb.RateLimiter.CreateResourceResponse\x12^\n\rAlterResource\x12%.Ydb.RateLimiter.AlterResourceRequest\x1a&.Ydb.RateLimiter.AlterResourceResponse\x12[\n\x0c\x44ropResource\x12$.Ydb.RateLimiter.DropResourceRequest\x1a%.Ydb.RateLimiter.DropResourceResponse\x12^\n\rListResources\x12%.Ydb.RateLimiter.ListResourcesRequest\x1a&.Ydb.RateLimiter.ListResourcesResponse\x12g\n\x10\x44\x65scribeResource\x12(.Ydb.RateLimiter.DescribeResourceRequest\x1a).Ydb.RateLimiter.DescribeResourceResponse\x12\x64\n\x0f\x41\x63quireResource\x12\'.Ydb.RateLimiter.AcquireResourceRequest\x1a(.Ydb.RateLimiter.AcquireResourceResponseB3\n\x1e\x63om.yandex.ydb.rate_limiter.v1B\x0fRateLimiterGrpcP\x01\x62\x06proto3'
  ,
  dependencies=[ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2.DESCRIPTOR,])



_sym_db.RegisterFileDescriptor(DESCRIPTOR)


DESCRIPTOR._options = None

_RATELIMITERSERVICE = _descriptor.ServiceDescriptor(
  name='RateLimiterService',
  full_name='Ydb.RateLimiter.V1.RateLimiterService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=116,
  serialized_end=727,
  methods=[
  _descriptor.MethodDescriptor(
    name='CreateResource',
    full_name='Ydb.RateLimiter.V1.RateLimiterService.CreateResource',
    index=0,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._CREATERESOURCEREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._CREATERESOURCERESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='AlterResource',
    full_name='Ydb.RateLimiter.V1.RateLimiterService.AlterResource',
    index=1,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._ALTERRESOURCEREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._ALTERRESOURCERESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='DropResource',
    full_name='Ydb.RateLimiter.V1.RateLimiterService.DropResource',
    index=2,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._DROPRESOURCEREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._DROPRESOURCERESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='ListResources',
    full_name='Ydb.RateLimiter.V1.RateLimiterService.ListResources',
    index=3,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._LISTRESOURCESREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._LISTRESOURCESRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='DescribeResource',
    full_name='Ydb.RateLimiter.V1.RateLimiterService.DescribeResource',
    index=4,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._DESCRIBERESOURCEREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._DESCRIBERESOURCERESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='AcquireResource',
    full_name='Ydb.RateLimiter.V1.RateLimiterService.AcquireResource',
    index=5,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._ACQUIRERESOURCEREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_ydb__rate__limiter__pb2._ACQUIRERESOURCERESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_RATELIMITERSERVICE)

DESCRIPTOR.services_by_name['RateLimiterService'] = _RATELIMITERSERVICE

# @@protoc_insertion_point(module_scope)
