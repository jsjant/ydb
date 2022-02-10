# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: ydb/public/api/grpc/draft/yql_db_v1.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from ydb.public.api.protos.draft import yq_private_pb2 as ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='ydb/public/api/grpc/draft/yql_db_v1.proto',
  package='Yq.Private.V1',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n)ydb/public/api/grpc/draft/yql_db_v1.proto\x12\rYq.Private.V1\x1a,ydb/public/api/protos/draft/yq_private.proto2\xdc\x02\n\x14YqPrivateTaskService\x12\x42\n\x07GetTask\x12\x1a.Yq.Private.GetTaskRequest\x1a\x1b.Yq.Private.GetTaskResponse\x12\x45\n\x08PingTask\x12\x1b.Yq.Private.PingTaskRequest\x1a\x1c.Yq.Private.PingTaskResponse\x12Z\n\x0fWriteTaskResult\x12\".Yq.Private.WriteTaskResultRequest\x1a#.Yq.Private.WriteTaskResultResponse\x12]\n\x10NodesHealthCheck\x12#.Yq.Private.NodesHealthCheckRequest\x1a$.Yq.Private.NodesHealthCheckResponseb\x06proto3'
  ,
  dependencies=[ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2.DESCRIPTOR,])



_sym_db.RegisterFileDescriptor(DESCRIPTOR)



_YQPRIVATETASKSERVICE = _descriptor.ServiceDescriptor(
  name='YqPrivateTaskService',
  full_name='Yq.Private.V1.YqPrivateTaskService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=107,
  serialized_end=455,
  methods=[
  _descriptor.MethodDescriptor(
    name='GetTask',
    full_name='Yq.Private.V1.YqPrivateTaskService.GetTask',
    index=0,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2._GETTASKREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2._GETTASKRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='PingTask',
    full_name='Yq.Private.V1.YqPrivateTaskService.PingTask',
    index=1,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2._PINGTASKREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2._PINGTASKRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='WriteTaskResult',
    full_name='Yq.Private.V1.YqPrivateTaskService.WriteTaskResult',
    index=2,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2._WRITETASKRESULTREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2._WRITETASKRESULTRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='NodesHealthCheck',
    full_name='Yq.Private.V1.YqPrivateTaskService.NodesHealthCheck',
    index=3,
    containing_service=None,
    input_type=ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2._NODESHEALTHCHECKREQUEST,
    output_type=ydb_dot_public_dot_api_dot_protos_dot_draft_dot_yq__private__pb2._NODESHEALTHCHECKRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_YQPRIVATETASKSERVICE)

DESCRIPTOR.services_by_name['YqPrivateTaskService'] = _YQPRIVATETASKSERVICE

# @@protoc_insertion_point(module_scope)
