package io.superdurable.gen;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Hosted by Dex server; SDKs call these RPCs.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.1)",
    comments = "Source: dex.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class FlowServiceGrpc {

  private FlowServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "dex.FlowService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      io.superdurable.gen.ServerInfo> getGetServerInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetServerInfo",
      requestType = com.google.protobuf.Empty.class,
      responseType = io.superdurable.gen.ServerInfo.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      io.superdurable.gen.ServerInfo> getGetServerInfoMethod() {
    io.grpc.MethodDescriptor<com.google.protobuf.Empty, io.superdurable.gen.ServerInfo> getGetServerInfoMethod;
    if ((getGetServerInfoMethod = FlowServiceGrpc.getGetServerInfoMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getGetServerInfoMethod = FlowServiceGrpc.getGetServerInfoMethod) == null) {
          FlowServiceGrpc.getGetServerInfoMethod = getGetServerInfoMethod =
              io.grpc.MethodDescriptor.<com.google.protobuf.Empty, io.superdurable.gen.ServerInfo>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetServerInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.ServerInfo.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("GetServerInfo"))
              .build();
        }
      }
    }
    return getGetServerInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.StartFlowRequest,
      io.superdurable.gen.StartFlowResponse> getStartFlowMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartFlow",
      requestType = io.superdurable.gen.StartFlowRequest.class,
      responseType = io.superdurable.gen.StartFlowResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.StartFlowRequest,
      io.superdurable.gen.StartFlowResponse> getStartFlowMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.StartFlowRequest, io.superdurable.gen.StartFlowResponse> getStartFlowMethod;
    if ((getStartFlowMethod = FlowServiceGrpc.getStartFlowMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getStartFlowMethod = FlowServiceGrpc.getStartFlowMethod) == null) {
          FlowServiceGrpc.getStartFlowMethod = getStartFlowMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.StartFlowRequest, io.superdurable.gen.StartFlowResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartFlow"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.StartFlowRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.StartFlowResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("StartFlow"))
              .build();
        }
      }
    }
    return getStartFlowMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.PublishToChannelRequest,
      com.google.protobuf.Empty> getPublishToChannelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PublishToChannel",
      requestType = io.superdurable.gen.PublishToChannelRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.PublishToChannelRequest,
      com.google.protobuf.Empty> getPublishToChannelMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.PublishToChannelRequest, com.google.protobuf.Empty> getPublishToChannelMethod;
    if ((getPublishToChannelMethod = FlowServiceGrpc.getPublishToChannelMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getPublishToChannelMethod = FlowServiceGrpc.getPublishToChannelMethod) == null) {
          FlowServiceGrpc.getPublishToChannelMethod = getPublishToChannelMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.PublishToChannelRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PublishToChannel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.PublishToChannelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("PublishToChannel"))
              .build();
        }
      }
    }
    return getPublishToChannelMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.GetChannelMessagesRequest,
      io.superdurable.gen.GetChannelMessagesResponse> getGetChannelMessagesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetChannelMessages",
      requestType = io.superdurable.gen.GetChannelMessagesRequest.class,
      responseType = io.superdurable.gen.GetChannelMessagesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.GetChannelMessagesRequest,
      io.superdurable.gen.GetChannelMessagesResponse> getGetChannelMessagesMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.GetChannelMessagesRequest, io.superdurable.gen.GetChannelMessagesResponse> getGetChannelMessagesMethod;
    if ((getGetChannelMessagesMethod = FlowServiceGrpc.getGetChannelMessagesMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getGetChannelMessagesMethod = FlowServiceGrpc.getGetChannelMessagesMethod) == null) {
          FlowServiceGrpc.getGetChannelMessagesMethod = getGetChannelMessagesMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.GetChannelMessagesRequest, io.superdurable.gen.GetChannelMessagesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetChannelMessages"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetChannelMessagesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetChannelMessagesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("GetChannelMessages"))
              .build();
        }
      }
    }
    return getGetChannelMessagesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.DeleteChannelMessageRequest,
      com.google.protobuf.Empty> getDeleteChannelMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteChannelMessage",
      requestType = io.superdurable.gen.DeleteChannelMessageRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.DeleteChannelMessageRequest,
      com.google.protobuf.Empty> getDeleteChannelMessageMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.DeleteChannelMessageRequest, com.google.protobuf.Empty> getDeleteChannelMessageMethod;
    if ((getDeleteChannelMessageMethod = FlowServiceGrpc.getDeleteChannelMessageMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getDeleteChannelMessageMethod = FlowServiceGrpc.getDeleteChannelMessageMethod) == null) {
          FlowServiceGrpc.getDeleteChannelMessageMethod = getDeleteChannelMessageMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.DeleteChannelMessageRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteChannelMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.DeleteChannelMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("DeleteChannelMessage"))
              .build();
        }
      }
    }
    return getDeleteChannelMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.WriteStreamRequest,
      com.google.protobuf.Empty> getWriteStreamMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WriteStream",
      requestType = io.superdurable.gen.WriteStreamRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.WriteStreamRequest,
      com.google.protobuf.Empty> getWriteStreamMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.WriteStreamRequest, com.google.protobuf.Empty> getWriteStreamMethod;
    if ((getWriteStreamMethod = FlowServiceGrpc.getWriteStreamMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getWriteStreamMethod = FlowServiceGrpc.getWriteStreamMethod) == null) {
          FlowServiceGrpc.getWriteStreamMethod = getWriteStreamMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.WriteStreamRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WriteStream"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.WriteStreamRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("WriteStream"))
              .build();
        }
      }
    }
    return getWriteStreamMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.ReadStreamRequest,
      io.superdurable.gen.ReadStreamResponse> getReadStreamMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadStream",
      requestType = io.superdurable.gen.ReadStreamRequest.class,
      responseType = io.superdurable.gen.ReadStreamResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.ReadStreamRequest,
      io.superdurable.gen.ReadStreamResponse> getReadStreamMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.ReadStreamRequest, io.superdurable.gen.ReadStreamResponse> getReadStreamMethod;
    if ((getReadStreamMethod = FlowServiceGrpc.getReadStreamMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getReadStreamMethod = FlowServiceGrpc.getReadStreamMethod) == null) {
          FlowServiceGrpc.getReadStreamMethod = getReadStreamMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.ReadStreamRequest, io.superdurable.gen.ReadStreamResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadStream"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.ReadStreamRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.ReadStreamResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("ReadStream"))
              .build();
        }
      }
    }
    return getReadStreamMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.ListStreamMessagesRequest,
      io.superdurable.gen.ListStreamMessagesResponse> getListStreamMessagesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListStreamMessages",
      requestType = io.superdurable.gen.ListStreamMessagesRequest.class,
      responseType = io.superdurable.gen.ListStreamMessagesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.ListStreamMessagesRequest,
      io.superdurable.gen.ListStreamMessagesResponse> getListStreamMessagesMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.ListStreamMessagesRequest, io.superdurable.gen.ListStreamMessagesResponse> getListStreamMessagesMethod;
    if ((getListStreamMessagesMethod = FlowServiceGrpc.getListStreamMessagesMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getListStreamMessagesMethod = FlowServiceGrpc.getListStreamMessagesMethod) == null) {
          FlowServiceGrpc.getListStreamMessagesMethod = getListStreamMessagesMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.ListStreamMessagesRequest, io.superdurable.gen.ListStreamMessagesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListStreamMessages"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.ListStreamMessagesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.ListStreamMessagesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("ListStreamMessages"))
              .build();
        }
      }
    }
    return getListStreamMessagesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.StopFlowRequest,
      com.google.protobuf.Empty> getStopFlowMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StopFlow",
      requestType = io.superdurable.gen.StopFlowRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.StopFlowRequest,
      com.google.protobuf.Empty> getStopFlowMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.StopFlowRequest, com.google.protobuf.Empty> getStopFlowMethod;
    if ((getStopFlowMethod = FlowServiceGrpc.getStopFlowMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getStopFlowMethod = FlowServiceGrpc.getStopFlowMethod) == null) {
          FlowServiceGrpc.getStopFlowMethod = getStopFlowMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.StopFlowRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StopFlow"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.StopFlowRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("StopFlow"))
              .build();
        }
      }
    }
    return getStopFlowMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.GetAttributesRequest,
      io.superdurable.gen.GetAttributesResponse> getGetAttributesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAttributes",
      requestType = io.superdurable.gen.GetAttributesRequest.class,
      responseType = io.superdurable.gen.GetAttributesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.GetAttributesRequest,
      io.superdurable.gen.GetAttributesResponse> getGetAttributesMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.GetAttributesRequest, io.superdurable.gen.GetAttributesResponse> getGetAttributesMethod;
    if ((getGetAttributesMethod = FlowServiceGrpc.getGetAttributesMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getGetAttributesMethod = FlowServiceGrpc.getGetAttributesMethod) == null) {
          FlowServiceGrpc.getGetAttributesMethod = getGetAttributesMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.GetAttributesRequest, io.superdurable.gen.GetAttributesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetAttributes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetAttributesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetAttributesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("GetAttributes"))
              .build();
        }
      }
    }
    return getGetAttributesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.SetAttributesRequest,
      com.google.protobuf.Empty> getSetAttributesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetAttributes",
      requestType = io.superdurable.gen.SetAttributesRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.SetAttributesRequest,
      com.google.protobuf.Empty> getSetAttributesMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.SetAttributesRequest, com.google.protobuf.Empty> getSetAttributesMethod;
    if ((getSetAttributesMethod = FlowServiceGrpc.getSetAttributesMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getSetAttributesMethod = FlowServiceGrpc.getSetAttributesMethod) == null) {
          FlowServiceGrpc.getSetAttributesMethod = getSetAttributesMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.SetAttributesRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetAttributes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.SetAttributesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("SetAttributes"))
              .build();
        }
      }
    }
    return getSetAttributesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.LoadBlobsRequest,
      io.superdurable.gen.LoadBlobsResponse> getLoadBlobsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "LoadBlobs",
      requestType = io.superdurable.gen.LoadBlobsRequest.class,
      responseType = io.superdurable.gen.LoadBlobsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.LoadBlobsRequest,
      io.superdurable.gen.LoadBlobsResponse> getLoadBlobsMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.LoadBlobsRequest, io.superdurable.gen.LoadBlobsResponse> getLoadBlobsMethod;
    if ((getLoadBlobsMethod = FlowServiceGrpc.getLoadBlobsMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getLoadBlobsMethod = FlowServiceGrpc.getLoadBlobsMethod) == null) {
          FlowServiceGrpc.getLoadBlobsMethod = getLoadBlobsMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.LoadBlobsRequest, io.superdurable.gen.LoadBlobsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "LoadBlobs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.LoadBlobsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.LoadBlobsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("LoadBlobs"))
              .build();
        }
      }
    }
    return getLoadBlobsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.WaitForFlowRequest,
      io.superdurable.gen.FlowResult> getWaitForFlowMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WaitForFlow",
      requestType = io.superdurable.gen.WaitForFlowRequest.class,
      responseType = io.superdurable.gen.FlowResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.WaitForFlowRequest,
      io.superdurable.gen.FlowResult> getWaitForFlowMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.WaitForFlowRequest, io.superdurable.gen.FlowResult> getWaitForFlowMethod;
    if ((getWaitForFlowMethod = FlowServiceGrpc.getWaitForFlowMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getWaitForFlowMethod = FlowServiceGrpc.getWaitForFlowMethod) == null) {
          FlowServiceGrpc.getWaitForFlowMethod = getWaitForFlowMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.WaitForFlowRequest, io.superdurable.gen.FlowResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WaitForFlow"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.WaitForFlowRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.FlowResult.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("WaitForFlow"))
              .build();
        }
      }
    }
    return getWaitForFlowMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.SearchFlowsRequest,
      io.superdurable.gen.SearchFlowsResponse> getSearchFlowsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SearchFlows",
      requestType = io.superdurable.gen.SearchFlowsRequest.class,
      responseType = io.superdurable.gen.SearchFlowsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.SearchFlowsRequest,
      io.superdurable.gen.SearchFlowsResponse> getSearchFlowsMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.SearchFlowsRequest, io.superdurable.gen.SearchFlowsResponse> getSearchFlowsMethod;
    if ((getSearchFlowsMethod = FlowServiceGrpc.getSearchFlowsMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getSearchFlowsMethod = FlowServiceGrpc.getSearchFlowsMethod) == null) {
          FlowServiceGrpc.getSearchFlowsMethod = getSearchFlowsMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.SearchFlowsRequest, io.superdurable.gen.SearchFlowsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SearchFlows"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.SearchFlowsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.SearchFlowsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("SearchFlows"))
              .build();
        }
      }
    }
    return getSearchFlowsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.SyncAttributeIndexRequest,
      io.superdurable.gen.SyncAttributeIndexResponse> getSyncAttributeIndexesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SyncAttributeIndexes",
      requestType = io.superdurable.gen.SyncAttributeIndexRequest.class,
      responseType = io.superdurable.gen.SyncAttributeIndexResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.SyncAttributeIndexRequest,
      io.superdurable.gen.SyncAttributeIndexResponse> getSyncAttributeIndexesMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.SyncAttributeIndexRequest, io.superdurable.gen.SyncAttributeIndexResponse> getSyncAttributeIndexesMethod;
    if ((getSyncAttributeIndexesMethod = FlowServiceGrpc.getSyncAttributeIndexesMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getSyncAttributeIndexesMethod = FlowServiceGrpc.getSyncAttributeIndexesMethod) == null) {
          FlowServiceGrpc.getSyncAttributeIndexesMethod = getSyncAttributeIndexesMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.SyncAttributeIndexRequest, io.superdurable.gen.SyncAttributeIndexResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SyncAttributeIndexes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.SyncAttributeIndexRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.SyncAttributeIndexResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("SyncAttributeIndexes"))
              .build();
        }
      }
    }
    return getSyncAttributeIndexesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.GetFlowSummaryRequest,
      io.superdurable.gen.GetFlowSummaryResponse> getGetFlowSummaryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetFlowSummary",
      requestType = io.superdurable.gen.GetFlowSummaryRequest.class,
      responseType = io.superdurable.gen.GetFlowSummaryResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.GetFlowSummaryRequest,
      io.superdurable.gen.GetFlowSummaryResponse> getGetFlowSummaryMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.GetFlowSummaryRequest, io.superdurable.gen.GetFlowSummaryResponse> getGetFlowSummaryMethod;
    if ((getGetFlowSummaryMethod = FlowServiceGrpc.getGetFlowSummaryMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getGetFlowSummaryMethod = FlowServiceGrpc.getGetFlowSummaryMethod) == null) {
          FlowServiceGrpc.getGetFlowSummaryMethod = getGetFlowSummaryMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.GetFlowSummaryRequest, io.superdurable.gen.GetFlowSummaryResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetFlowSummary"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetFlowSummaryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetFlowSummaryResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("GetFlowSummary"))
              .build();
        }
      }
    }
    return getGetFlowSummaryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.GetHistoryEventsRequest,
      io.superdurable.gen.GetHistoryEventsResponse> getGetHistoryEventsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetHistoryEvents",
      requestType = io.superdurable.gen.GetHistoryEventsRequest.class,
      responseType = io.superdurable.gen.GetHistoryEventsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.GetHistoryEventsRequest,
      io.superdurable.gen.GetHistoryEventsResponse> getGetHistoryEventsMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.GetHistoryEventsRequest, io.superdurable.gen.GetHistoryEventsResponse> getGetHistoryEventsMethod;
    if ((getGetHistoryEventsMethod = FlowServiceGrpc.getGetHistoryEventsMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getGetHistoryEventsMethod = FlowServiceGrpc.getGetHistoryEventsMethod) == null) {
          FlowServiceGrpc.getGetHistoryEventsMethod = getGetHistoryEventsMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.GetHistoryEventsRequest, io.superdurable.gen.GetHistoryEventsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetHistoryEvents"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetHistoryEventsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetHistoryEventsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("GetHistoryEvents"))
              .build();
        }
      }
    }
    return getGetHistoryEventsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.WaitForHistoryEventRequest,
      io.superdurable.gen.WaitForHistoryEventResponse> getWaitForHistoryEventMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WaitForHistoryEvent",
      requestType = io.superdurable.gen.WaitForHistoryEventRequest.class,
      responseType = io.superdurable.gen.WaitForHistoryEventResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.WaitForHistoryEventRequest,
      io.superdurable.gen.WaitForHistoryEventResponse> getWaitForHistoryEventMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.WaitForHistoryEventRequest, io.superdurable.gen.WaitForHistoryEventResponse> getWaitForHistoryEventMethod;
    if ((getWaitForHistoryEventMethod = FlowServiceGrpc.getWaitForHistoryEventMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getWaitForHistoryEventMethod = FlowServiceGrpc.getWaitForHistoryEventMethod) == null) {
          FlowServiceGrpc.getWaitForHistoryEventMethod = getWaitForHistoryEventMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.WaitForHistoryEventRequest, io.superdurable.gen.WaitForHistoryEventResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WaitForHistoryEvent"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.WaitForHistoryEventRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.WaitForHistoryEventResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("WaitForHistoryEvent"))
              .build();
        }
      }
    }
    return getWaitForHistoryEventMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.GetFlowStateRequest,
      io.superdurable.gen.GetFlowStateResponse> getGetFlowStateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetFlowState",
      requestType = io.superdurable.gen.GetFlowStateRequest.class,
      responseType = io.superdurable.gen.GetFlowStateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.GetFlowStateRequest,
      io.superdurable.gen.GetFlowStateResponse> getGetFlowStateMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.GetFlowStateRequest, io.superdurable.gen.GetFlowStateResponse> getGetFlowStateMethod;
    if ((getGetFlowStateMethod = FlowServiceGrpc.getGetFlowStateMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getGetFlowStateMethod = FlowServiceGrpc.getGetFlowStateMethod) == null) {
          FlowServiceGrpc.getGetFlowStateMethod = getGetFlowStateMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.GetFlowStateRequest, io.superdurable.gen.GetFlowStateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetFlowState"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetFlowStateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.GetFlowStateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("GetFlowState"))
              .build();
        }
      }
    }
    return getGetFlowStateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.ResetFlowRequest,
      io.superdurable.gen.ResetFlowResponse> getResetFlowMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResetFlow",
      requestType = io.superdurable.gen.ResetFlowRequest.class,
      responseType = io.superdurable.gen.ResetFlowResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.ResetFlowRequest,
      io.superdurable.gen.ResetFlowResponse> getResetFlowMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.ResetFlowRequest, io.superdurable.gen.ResetFlowResponse> getResetFlowMethod;
    if ((getResetFlowMethod = FlowServiceGrpc.getResetFlowMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getResetFlowMethod = FlowServiceGrpc.getResetFlowMethod) == null) {
          FlowServiceGrpc.getResetFlowMethod = getResetFlowMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.ResetFlowRequest, io.superdurable.gen.ResetFlowResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResetFlow"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.ResetFlowRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.ResetFlowResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("ResetFlow"))
              .build();
        }
      }
    }
    return getResetFlowMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.InvokeRPCRequest,
      io.superdurable.gen.InvokeRPCResponse> getInvokeRPCMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "InvokeRPC",
      requestType = io.superdurable.gen.InvokeRPCRequest.class,
      responseType = io.superdurable.gen.InvokeRPCResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.InvokeRPCRequest,
      io.superdurable.gen.InvokeRPCResponse> getInvokeRPCMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.InvokeRPCRequest, io.superdurable.gen.InvokeRPCResponse> getInvokeRPCMethod;
    if ((getInvokeRPCMethod = FlowServiceGrpc.getInvokeRPCMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getInvokeRPCMethod = FlowServiceGrpc.getInvokeRPCMethod) == null) {
          FlowServiceGrpc.getInvokeRPCMethod = getInvokeRPCMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.InvokeRPCRequest, io.superdurable.gen.InvokeRPCResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "InvokeRPC"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.InvokeRPCRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.InvokeRPCResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("InvokeRPC"))
              .build();
        }
      }
    }
    return getInvokeRPCMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.SkipTimerRequest,
      com.google.protobuf.Empty> getSkipTimerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SkipTimer",
      requestType = io.superdurable.gen.SkipTimerRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.SkipTimerRequest,
      com.google.protobuf.Empty> getSkipTimerMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.SkipTimerRequest, com.google.protobuf.Empty> getSkipTimerMethod;
    if ((getSkipTimerMethod = FlowServiceGrpc.getSkipTimerMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getSkipTimerMethod = FlowServiceGrpc.getSkipTimerMethod) == null) {
          FlowServiceGrpc.getSkipTimerMethod = getSkipTimerMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.SkipTimerRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SkipTimer"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.SkipTimerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("SkipTimer"))
              .build();
        }
      }
    }
    return getSkipTimerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.UpdateFlowConfigRequest,
      com.google.protobuf.Empty> getUpdateFlowConfigMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateFlowConfig",
      requestType = io.superdurable.gen.UpdateFlowConfigRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.UpdateFlowConfigRequest,
      com.google.protobuf.Empty> getUpdateFlowConfigMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.UpdateFlowConfigRequest, com.google.protobuf.Empty> getUpdateFlowConfigMethod;
    if ((getUpdateFlowConfigMethod = FlowServiceGrpc.getUpdateFlowConfigMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getUpdateFlowConfigMethod = FlowServiceGrpc.getUpdateFlowConfigMethod) == null) {
          FlowServiceGrpc.getUpdateFlowConfigMethod = getUpdateFlowConfigMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.UpdateFlowConfigRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateFlowConfig"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.UpdateFlowConfigRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("UpdateFlowConfig"))
              .build();
        }
      }
    }
    return getUpdateFlowConfigMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.WaitForStepCompletionRequest,
      io.superdurable.gen.WaitForStepCompletionResponse> getWaitForStepCompletionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WaitForStepCompletion",
      requestType = io.superdurable.gen.WaitForStepCompletionRequest.class,
      responseType = io.superdurable.gen.WaitForStepCompletionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.WaitForStepCompletionRequest,
      io.superdurable.gen.WaitForStepCompletionResponse> getWaitForStepCompletionMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.WaitForStepCompletionRequest, io.superdurable.gen.WaitForStepCompletionResponse> getWaitForStepCompletionMethod;
    if ((getWaitForStepCompletionMethod = FlowServiceGrpc.getWaitForStepCompletionMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getWaitForStepCompletionMethod = FlowServiceGrpc.getWaitForStepCompletionMethod) == null) {
          FlowServiceGrpc.getWaitForStepCompletionMethod = getWaitForStepCompletionMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.WaitForStepCompletionRequest, io.superdurable.gen.WaitForStepCompletionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WaitForStepCompletion"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.WaitForStepCompletionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.WaitForStepCompletionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("WaitForStepCompletion"))
              .build();
        }
      }
    }
    return getWaitForStepCompletionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.WaitForAttributeRequest,
      io.superdurable.gen.WaitForAttributeResponse> getWaitForAttributeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WaitForAttribute",
      requestType = io.superdurable.gen.WaitForAttributeRequest.class,
      responseType = io.superdurable.gen.WaitForAttributeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.WaitForAttributeRequest,
      io.superdurable.gen.WaitForAttributeResponse> getWaitForAttributeMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.WaitForAttributeRequest, io.superdurable.gen.WaitForAttributeResponse> getWaitForAttributeMethod;
    if ((getWaitForAttributeMethod = FlowServiceGrpc.getWaitForAttributeMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getWaitForAttributeMethod = FlowServiceGrpc.getWaitForAttributeMethod) == null) {
          FlowServiceGrpc.getWaitForAttributeMethod = getWaitForAttributeMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.WaitForAttributeRequest, io.superdurable.gen.WaitForAttributeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WaitForAttribute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.WaitForAttributeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.WaitForAttributeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("WaitForAttribute"))
              .build();
        }
      }
    }
    return getWaitForAttributeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.TriggerContinueAsNewRequest,
      com.google.protobuf.Empty> getTriggerContinueAsNewMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TriggerContinueAsNew",
      requestType = io.superdurable.gen.TriggerContinueAsNewRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.TriggerContinueAsNewRequest,
      com.google.protobuf.Empty> getTriggerContinueAsNewMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.TriggerContinueAsNewRequest, com.google.protobuf.Empty> getTriggerContinueAsNewMethod;
    if ((getTriggerContinueAsNewMethod = FlowServiceGrpc.getTriggerContinueAsNewMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getTriggerContinueAsNewMethod = FlowServiceGrpc.getTriggerContinueAsNewMethod) == null) {
          FlowServiceGrpc.getTriggerContinueAsNewMethod = getTriggerContinueAsNewMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.TriggerContinueAsNewRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TriggerContinueAsNew"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.TriggerContinueAsNewRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("TriggerContinueAsNew"))
              .build();
        }
      }
    }
    return getTriggerContinueAsNewMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      io.superdurable.gen.HealthInfo> getHealthCheckMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HealthCheck",
      requestType = com.google.protobuf.Empty.class,
      responseType = io.superdurable.gen.HealthInfo.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      io.superdurable.gen.HealthInfo> getHealthCheckMethod() {
    io.grpc.MethodDescriptor<com.google.protobuf.Empty, io.superdurable.gen.HealthInfo> getHealthCheckMethod;
    if ((getHealthCheckMethod = FlowServiceGrpc.getHealthCheckMethod) == null) {
      synchronized (FlowServiceGrpc.class) {
        if ((getHealthCheckMethod = FlowServiceGrpc.getHealthCheckMethod) == null) {
          FlowServiceGrpc.getHealthCheckMethod = getHealthCheckMethod =
              io.grpc.MethodDescriptor.<com.google.protobuf.Empty, io.superdurable.gen.HealthInfo>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HealthCheck"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.HealthInfo.getDefaultInstance()))
              .setSchemaDescriptor(new FlowServiceMethodDescriptorSupplier("HealthCheck"))
              .build();
        }
      }
    }
    return getHealthCheckMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FlowServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FlowServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FlowServiceStub>() {
        @java.lang.Override
        public FlowServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FlowServiceStub(channel, callOptions);
        }
      };
    return FlowServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FlowServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FlowServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FlowServiceBlockingStub>() {
        @java.lang.Override
        public FlowServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FlowServiceBlockingStub(channel, callOptions);
        }
      };
    return FlowServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FlowServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FlowServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FlowServiceFutureStub>() {
        @java.lang.Override
        public FlowServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FlowServiceFutureStub(channel, callOptions);
        }
      };
    return FlowServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Hosted by Dex server; SDKs call these RPCs.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Returns diagnostic release metadata and the Server's inclusive protocol interval.
     * </pre>
     */
    default void getServerInfo(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ServerInfo> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetServerInfoMethod(), responseObserver);
    }

    /**
     */
    default void startFlow(io.superdurable.gen.StartFlowRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.StartFlowResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartFlowMethod(), responseObserver);
    }

    /**
     */
    default void publishToChannel(io.superdurable.gen.PublishToChannelRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPublishToChannelMethod(), responseObserver);
    }

    /**
     */
    default void getChannelMessages(io.superdurable.gen.GetChannelMessagesRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetChannelMessagesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetChannelMessagesMethod(), responseObserver);
    }

    /**
     */
    default void deleteChannelMessage(io.superdurable.gen.DeleteChannelMessageRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteChannelMessageMethod(), responseObserver);
    }

    /**
     */
    default void writeStream(io.superdurable.gen.WriteStreamRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWriteStreamMethod(), responseObserver);
    }

    /**
     */
    default void readStream(io.superdurable.gen.ReadStreamRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ReadStreamResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadStreamMethod(), responseObserver);
    }

    /**
     */
    default void listStreamMessages(io.superdurable.gen.ListStreamMessagesRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ListStreamMessagesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListStreamMessagesMethod(), responseObserver);
    }

    /**
     */
    default void stopFlow(io.superdurable.gen.StopFlowRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStopFlowMethod(), responseObserver);
    }

    /**
     */
    default void getAttributes(io.superdurable.gen.GetAttributesRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetAttributesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetAttributesMethod(), responseObserver);
    }

    /**
     */
    default void setAttributes(io.superdurable.gen.SetAttributesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetAttributesMethod(), responseObserver);
    }

    /**
     */
    default void loadBlobs(io.superdurable.gen.LoadBlobsRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.LoadBlobsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLoadBlobsMethod(), responseObserver);
    }

    /**
     */
    default void waitForFlow(io.superdurable.gen.WaitForFlowRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.FlowResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWaitForFlowMethod(), responseObserver);
    }

    /**
     */
    default void searchFlows(io.superdurable.gen.SearchFlowsRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.SearchFlowsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSearchFlowsMethod(), responseObserver);
    }

    /**
     */
    default void syncAttributeIndexes(io.superdurable.gen.SyncAttributeIndexRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.SyncAttributeIndexResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSyncAttributeIndexesMethod(), responseObserver);
    }

    /**
     */
    default void getFlowSummary(io.superdurable.gen.GetFlowSummaryRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetFlowSummaryResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetFlowSummaryMethod(), responseObserver);
    }

    /**
     */
    default void getHistoryEvents(io.superdurable.gen.GetHistoryEventsRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetHistoryEventsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetHistoryEventsMethod(), responseObserver);
    }

    /**
     */
    default void waitForHistoryEvent(io.superdurable.gen.WaitForHistoryEventRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.WaitForHistoryEventResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWaitForHistoryEventMethod(), responseObserver);
    }

    /**
     */
    default void getFlowState(io.superdurable.gen.GetFlowStateRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetFlowStateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetFlowStateMethod(), responseObserver);
    }

    /**
     */
    default void resetFlow(io.superdurable.gen.ResetFlowRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ResetFlowResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResetFlowMethod(), responseObserver);
    }

    /**
     */
    default void invokeRPC(io.superdurable.gen.InvokeRPCRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeRPCResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInvokeRPCMethod(), responseObserver);
    }

    /**
     */
    default void skipTimer(io.superdurable.gen.SkipTimerRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSkipTimerMethod(), responseObserver);
    }

    /**
     */
    default void updateFlowConfig(io.superdurable.gen.UpdateFlowConfigRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateFlowConfigMethod(), responseObserver);
    }

    /**
     */
    default void waitForStepCompletion(io.superdurable.gen.WaitForStepCompletionRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.WaitForStepCompletionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWaitForStepCompletionMethod(), responseObserver);
    }

    /**
     */
    default void waitForAttribute(io.superdurable.gen.WaitForAttributeRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.WaitForAttributeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWaitForAttributeMethod(), responseObserver);
    }

    /**
     * <pre>
     * server-zd returns UNIMPLEMENTED.
     * </pre>
     */
    default void triggerContinueAsNew(io.superdurable.gen.TriggerContinueAsNewRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTriggerContinueAsNewMethod(), responseObserver);
    }

    /**
     */
    default void healthCheck(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.HealthInfo> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHealthCheckMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service FlowService.
   * <pre>
   * Hosted by Dex server; SDKs call these RPCs.
   * </pre>
   */
  public static abstract class FlowServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return FlowServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service FlowService.
   * <pre>
   * Hosted by Dex server; SDKs call these RPCs.
   * </pre>
   */
  public static final class FlowServiceStub
      extends io.grpc.stub.AbstractAsyncStub<FlowServiceStub> {
    private FlowServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FlowServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FlowServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Returns diagnostic release metadata and the Server's inclusive protocol interval.
     * </pre>
     */
    public void getServerInfo(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ServerInfo> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetServerInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void startFlow(io.superdurable.gen.StartFlowRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.StartFlowResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartFlowMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void publishToChannel(io.superdurable.gen.PublishToChannelRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPublishToChannelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getChannelMessages(io.superdurable.gen.GetChannelMessagesRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetChannelMessagesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetChannelMessagesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteChannelMessage(io.superdurable.gen.DeleteChannelMessageRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteChannelMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void writeStream(io.superdurable.gen.WriteStreamRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWriteStreamMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void readStream(io.superdurable.gen.ReadStreamRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ReadStreamResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadStreamMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listStreamMessages(io.superdurable.gen.ListStreamMessagesRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ListStreamMessagesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListStreamMessagesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void stopFlow(io.superdurable.gen.StopFlowRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStopFlowMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getAttributes(io.superdurable.gen.GetAttributesRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetAttributesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetAttributesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void setAttributes(io.superdurable.gen.SetAttributesRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetAttributesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void loadBlobs(io.superdurable.gen.LoadBlobsRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.LoadBlobsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLoadBlobsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void waitForFlow(io.superdurable.gen.WaitForFlowRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.FlowResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWaitForFlowMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void searchFlows(io.superdurable.gen.SearchFlowsRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.SearchFlowsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSearchFlowsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void syncAttributeIndexes(io.superdurable.gen.SyncAttributeIndexRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.SyncAttributeIndexResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSyncAttributeIndexesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getFlowSummary(io.superdurable.gen.GetFlowSummaryRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetFlowSummaryResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetFlowSummaryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getHistoryEvents(io.superdurable.gen.GetHistoryEventsRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetHistoryEventsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetHistoryEventsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void waitForHistoryEvent(io.superdurable.gen.WaitForHistoryEventRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.WaitForHistoryEventResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWaitForHistoryEventMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getFlowState(io.superdurable.gen.GetFlowStateRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.GetFlowStateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetFlowStateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void resetFlow(io.superdurable.gen.ResetFlowRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ResetFlowResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResetFlowMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void invokeRPC(io.superdurable.gen.InvokeRPCRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeRPCResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getInvokeRPCMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void skipTimer(io.superdurable.gen.SkipTimerRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSkipTimerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateFlowConfig(io.superdurable.gen.UpdateFlowConfigRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateFlowConfigMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void waitForStepCompletion(io.superdurable.gen.WaitForStepCompletionRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.WaitForStepCompletionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWaitForStepCompletionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void waitForAttribute(io.superdurable.gen.WaitForAttributeRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.WaitForAttributeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWaitForAttributeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * server-zd returns UNIMPLEMENTED.
     * </pre>
     */
    public void triggerContinueAsNew(io.superdurable.gen.TriggerContinueAsNewRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTriggerContinueAsNewMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void healthCheck(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.HealthInfo> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service FlowService.
   * <pre>
   * Hosted by Dex server; SDKs call these RPCs.
   * </pre>
   */
  public static final class FlowServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<FlowServiceBlockingStub> {
    private FlowServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FlowServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FlowServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Returns diagnostic release metadata and the Server's inclusive protocol interval.
     * </pre>
     */
    public io.superdurable.gen.ServerInfo getServerInfo(com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetServerInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.StartFlowResponse startFlow(io.superdurable.gen.StartFlowRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartFlowMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty publishToChannel(io.superdurable.gen.PublishToChannelRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPublishToChannelMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.GetChannelMessagesResponse getChannelMessages(io.superdurable.gen.GetChannelMessagesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetChannelMessagesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty deleteChannelMessage(io.superdurable.gen.DeleteChannelMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteChannelMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty writeStream(io.superdurable.gen.WriteStreamRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWriteStreamMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.ReadStreamResponse readStream(io.superdurable.gen.ReadStreamRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadStreamMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.ListStreamMessagesResponse listStreamMessages(io.superdurable.gen.ListStreamMessagesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListStreamMessagesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty stopFlow(io.superdurable.gen.StopFlowRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStopFlowMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.GetAttributesResponse getAttributes(io.superdurable.gen.GetAttributesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetAttributesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty setAttributes(io.superdurable.gen.SetAttributesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetAttributesMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.LoadBlobsResponse loadBlobs(io.superdurable.gen.LoadBlobsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLoadBlobsMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.FlowResult waitForFlow(io.superdurable.gen.WaitForFlowRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWaitForFlowMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.SearchFlowsResponse searchFlows(io.superdurable.gen.SearchFlowsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSearchFlowsMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.SyncAttributeIndexResponse syncAttributeIndexes(io.superdurable.gen.SyncAttributeIndexRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSyncAttributeIndexesMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.GetFlowSummaryResponse getFlowSummary(io.superdurable.gen.GetFlowSummaryRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetFlowSummaryMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.GetHistoryEventsResponse getHistoryEvents(io.superdurable.gen.GetHistoryEventsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetHistoryEventsMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.WaitForHistoryEventResponse waitForHistoryEvent(io.superdurable.gen.WaitForHistoryEventRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWaitForHistoryEventMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.GetFlowStateResponse getFlowState(io.superdurable.gen.GetFlowStateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetFlowStateMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.ResetFlowResponse resetFlow(io.superdurable.gen.ResetFlowRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResetFlowMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.InvokeRPCResponse invokeRPC(io.superdurable.gen.InvokeRPCRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getInvokeRPCMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty skipTimer(io.superdurable.gen.SkipTimerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSkipTimerMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty updateFlowConfig(io.superdurable.gen.UpdateFlowConfigRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateFlowConfigMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.WaitForStepCompletionResponse waitForStepCompletion(io.superdurable.gen.WaitForStepCompletionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWaitForStepCompletionMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.WaitForAttributeResponse waitForAttribute(io.superdurable.gen.WaitForAttributeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWaitForAttributeMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * server-zd returns UNIMPLEMENTED.
     * </pre>
     */
    public com.google.protobuf.Empty triggerContinueAsNew(io.superdurable.gen.TriggerContinueAsNewRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTriggerContinueAsNewMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.HealthInfo healthCheck(com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHealthCheckMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service FlowService.
   * <pre>
   * Hosted by Dex server; SDKs call these RPCs.
   * </pre>
   */
  public static final class FlowServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<FlowServiceFutureStub> {
    private FlowServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FlowServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FlowServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Returns diagnostic release metadata and the Server's inclusive protocol interval.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.ServerInfo> getServerInfo(
        com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetServerInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.StartFlowResponse> startFlow(
        io.superdurable.gen.StartFlowRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartFlowMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> publishToChannel(
        io.superdurable.gen.PublishToChannelRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPublishToChannelMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.GetChannelMessagesResponse> getChannelMessages(
        io.superdurable.gen.GetChannelMessagesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetChannelMessagesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteChannelMessage(
        io.superdurable.gen.DeleteChannelMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteChannelMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> writeStream(
        io.superdurable.gen.WriteStreamRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWriteStreamMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.ReadStreamResponse> readStream(
        io.superdurable.gen.ReadStreamRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadStreamMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.ListStreamMessagesResponse> listStreamMessages(
        io.superdurable.gen.ListStreamMessagesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListStreamMessagesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> stopFlow(
        io.superdurable.gen.StopFlowRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStopFlowMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.GetAttributesResponse> getAttributes(
        io.superdurable.gen.GetAttributesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetAttributesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> setAttributes(
        io.superdurable.gen.SetAttributesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetAttributesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.LoadBlobsResponse> loadBlobs(
        io.superdurable.gen.LoadBlobsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLoadBlobsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.FlowResult> waitForFlow(
        io.superdurable.gen.WaitForFlowRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWaitForFlowMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.SearchFlowsResponse> searchFlows(
        io.superdurable.gen.SearchFlowsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSearchFlowsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.SyncAttributeIndexResponse> syncAttributeIndexes(
        io.superdurable.gen.SyncAttributeIndexRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSyncAttributeIndexesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.GetFlowSummaryResponse> getFlowSummary(
        io.superdurable.gen.GetFlowSummaryRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetFlowSummaryMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.GetHistoryEventsResponse> getHistoryEvents(
        io.superdurable.gen.GetHistoryEventsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetHistoryEventsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.WaitForHistoryEventResponse> waitForHistoryEvent(
        io.superdurable.gen.WaitForHistoryEventRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWaitForHistoryEventMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.GetFlowStateResponse> getFlowState(
        io.superdurable.gen.GetFlowStateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetFlowStateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.ResetFlowResponse> resetFlow(
        io.superdurable.gen.ResetFlowRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResetFlowMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.InvokeRPCResponse> invokeRPC(
        io.superdurable.gen.InvokeRPCRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getInvokeRPCMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> skipTimer(
        io.superdurable.gen.SkipTimerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSkipTimerMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> updateFlowConfig(
        io.superdurable.gen.UpdateFlowConfigRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateFlowConfigMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.WaitForStepCompletionResponse> waitForStepCompletion(
        io.superdurable.gen.WaitForStepCompletionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWaitForStepCompletionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.WaitForAttributeResponse> waitForAttribute(
        io.superdurable.gen.WaitForAttributeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWaitForAttributeMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * server-zd returns UNIMPLEMENTED.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> triggerContinueAsNew(
        io.superdurable.gen.TriggerContinueAsNewRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTriggerContinueAsNewMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.HealthInfo> healthCheck(
        com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_SERVER_INFO = 0;
  private static final int METHODID_START_FLOW = 1;
  private static final int METHODID_PUBLISH_TO_CHANNEL = 2;
  private static final int METHODID_GET_CHANNEL_MESSAGES = 3;
  private static final int METHODID_DELETE_CHANNEL_MESSAGE = 4;
  private static final int METHODID_WRITE_STREAM = 5;
  private static final int METHODID_READ_STREAM = 6;
  private static final int METHODID_LIST_STREAM_MESSAGES = 7;
  private static final int METHODID_STOP_FLOW = 8;
  private static final int METHODID_GET_ATTRIBUTES = 9;
  private static final int METHODID_SET_ATTRIBUTES = 10;
  private static final int METHODID_LOAD_BLOBS = 11;
  private static final int METHODID_WAIT_FOR_FLOW = 12;
  private static final int METHODID_SEARCH_FLOWS = 13;
  private static final int METHODID_SYNC_ATTRIBUTE_INDEXES = 14;
  private static final int METHODID_GET_FLOW_SUMMARY = 15;
  private static final int METHODID_GET_HISTORY_EVENTS = 16;
  private static final int METHODID_WAIT_FOR_HISTORY_EVENT = 17;
  private static final int METHODID_GET_FLOW_STATE = 18;
  private static final int METHODID_RESET_FLOW = 19;
  private static final int METHODID_INVOKE_RPC = 20;
  private static final int METHODID_SKIP_TIMER = 21;
  private static final int METHODID_UPDATE_FLOW_CONFIG = 22;
  private static final int METHODID_WAIT_FOR_STEP_COMPLETION = 23;
  private static final int METHODID_WAIT_FOR_ATTRIBUTE = 24;
  private static final int METHODID_TRIGGER_CONTINUE_AS_NEW = 25;
  private static final int METHODID_HEALTH_CHECK = 26;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_SERVER_INFO:
          serviceImpl.getServerInfo((com.google.protobuf.Empty) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.ServerInfo>) responseObserver);
          break;
        case METHODID_START_FLOW:
          serviceImpl.startFlow((io.superdurable.gen.StartFlowRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.StartFlowResponse>) responseObserver);
          break;
        case METHODID_PUBLISH_TO_CHANNEL:
          serviceImpl.publishToChannel((io.superdurable.gen.PublishToChannelRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_GET_CHANNEL_MESSAGES:
          serviceImpl.getChannelMessages((io.superdurable.gen.GetChannelMessagesRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.GetChannelMessagesResponse>) responseObserver);
          break;
        case METHODID_DELETE_CHANNEL_MESSAGE:
          serviceImpl.deleteChannelMessage((io.superdurable.gen.DeleteChannelMessageRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_WRITE_STREAM:
          serviceImpl.writeStream((io.superdurable.gen.WriteStreamRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_READ_STREAM:
          serviceImpl.readStream((io.superdurable.gen.ReadStreamRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.ReadStreamResponse>) responseObserver);
          break;
        case METHODID_LIST_STREAM_MESSAGES:
          serviceImpl.listStreamMessages((io.superdurable.gen.ListStreamMessagesRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.ListStreamMessagesResponse>) responseObserver);
          break;
        case METHODID_STOP_FLOW:
          serviceImpl.stopFlow((io.superdurable.gen.StopFlowRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_GET_ATTRIBUTES:
          serviceImpl.getAttributes((io.superdurable.gen.GetAttributesRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.GetAttributesResponse>) responseObserver);
          break;
        case METHODID_SET_ATTRIBUTES:
          serviceImpl.setAttributes((io.superdurable.gen.SetAttributesRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_LOAD_BLOBS:
          serviceImpl.loadBlobs((io.superdurable.gen.LoadBlobsRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.LoadBlobsResponse>) responseObserver);
          break;
        case METHODID_WAIT_FOR_FLOW:
          serviceImpl.waitForFlow((io.superdurable.gen.WaitForFlowRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.FlowResult>) responseObserver);
          break;
        case METHODID_SEARCH_FLOWS:
          serviceImpl.searchFlows((io.superdurable.gen.SearchFlowsRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.SearchFlowsResponse>) responseObserver);
          break;
        case METHODID_SYNC_ATTRIBUTE_INDEXES:
          serviceImpl.syncAttributeIndexes((io.superdurable.gen.SyncAttributeIndexRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.SyncAttributeIndexResponse>) responseObserver);
          break;
        case METHODID_GET_FLOW_SUMMARY:
          serviceImpl.getFlowSummary((io.superdurable.gen.GetFlowSummaryRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.GetFlowSummaryResponse>) responseObserver);
          break;
        case METHODID_GET_HISTORY_EVENTS:
          serviceImpl.getHistoryEvents((io.superdurable.gen.GetHistoryEventsRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.GetHistoryEventsResponse>) responseObserver);
          break;
        case METHODID_WAIT_FOR_HISTORY_EVENT:
          serviceImpl.waitForHistoryEvent((io.superdurable.gen.WaitForHistoryEventRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.WaitForHistoryEventResponse>) responseObserver);
          break;
        case METHODID_GET_FLOW_STATE:
          serviceImpl.getFlowState((io.superdurable.gen.GetFlowStateRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.GetFlowStateResponse>) responseObserver);
          break;
        case METHODID_RESET_FLOW:
          serviceImpl.resetFlow((io.superdurable.gen.ResetFlowRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.ResetFlowResponse>) responseObserver);
          break;
        case METHODID_INVOKE_RPC:
          serviceImpl.invokeRPC((io.superdurable.gen.InvokeRPCRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeRPCResponse>) responseObserver);
          break;
        case METHODID_SKIP_TIMER:
          serviceImpl.skipTimer((io.superdurable.gen.SkipTimerRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_UPDATE_FLOW_CONFIG:
          serviceImpl.updateFlowConfig((io.superdurable.gen.UpdateFlowConfigRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_WAIT_FOR_STEP_COMPLETION:
          serviceImpl.waitForStepCompletion((io.superdurable.gen.WaitForStepCompletionRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.WaitForStepCompletionResponse>) responseObserver);
          break;
        case METHODID_WAIT_FOR_ATTRIBUTE:
          serviceImpl.waitForAttribute((io.superdurable.gen.WaitForAttributeRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.WaitForAttributeResponse>) responseObserver);
          break;
        case METHODID_TRIGGER_CONTINUE_AS_NEW:
          serviceImpl.triggerContinueAsNew((io.superdurable.gen.TriggerContinueAsNewRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_HEALTH_CHECK:
          serviceImpl.healthCheck((com.google.protobuf.Empty) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.HealthInfo>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getGetServerInfoMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.protobuf.Empty,
              io.superdurable.gen.ServerInfo>(
                service, METHODID_GET_SERVER_INFO)))
        .addMethod(
          getStartFlowMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.StartFlowRequest,
              io.superdurable.gen.StartFlowResponse>(
                service, METHODID_START_FLOW)))
        .addMethod(
          getPublishToChannelMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.PublishToChannelRequest,
              com.google.protobuf.Empty>(
                service, METHODID_PUBLISH_TO_CHANNEL)))
        .addMethod(
          getGetChannelMessagesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.GetChannelMessagesRequest,
              io.superdurable.gen.GetChannelMessagesResponse>(
                service, METHODID_GET_CHANNEL_MESSAGES)))
        .addMethod(
          getDeleteChannelMessageMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.DeleteChannelMessageRequest,
              com.google.protobuf.Empty>(
                service, METHODID_DELETE_CHANNEL_MESSAGE)))
        .addMethod(
          getWriteStreamMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.WriteStreamRequest,
              com.google.protobuf.Empty>(
                service, METHODID_WRITE_STREAM)))
        .addMethod(
          getReadStreamMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.ReadStreamRequest,
              io.superdurable.gen.ReadStreamResponse>(
                service, METHODID_READ_STREAM)))
        .addMethod(
          getListStreamMessagesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.ListStreamMessagesRequest,
              io.superdurable.gen.ListStreamMessagesResponse>(
                service, METHODID_LIST_STREAM_MESSAGES)))
        .addMethod(
          getStopFlowMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.StopFlowRequest,
              com.google.protobuf.Empty>(
                service, METHODID_STOP_FLOW)))
        .addMethod(
          getGetAttributesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.GetAttributesRequest,
              io.superdurable.gen.GetAttributesResponse>(
                service, METHODID_GET_ATTRIBUTES)))
        .addMethod(
          getSetAttributesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.SetAttributesRequest,
              com.google.protobuf.Empty>(
                service, METHODID_SET_ATTRIBUTES)))
        .addMethod(
          getLoadBlobsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.LoadBlobsRequest,
              io.superdurable.gen.LoadBlobsResponse>(
                service, METHODID_LOAD_BLOBS)))
        .addMethod(
          getWaitForFlowMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.WaitForFlowRequest,
              io.superdurable.gen.FlowResult>(
                service, METHODID_WAIT_FOR_FLOW)))
        .addMethod(
          getSearchFlowsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.SearchFlowsRequest,
              io.superdurable.gen.SearchFlowsResponse>(
                service, METHODID_SEARCH_FLOWS)))
        .addMethod(
          getSyncAttributeIndexesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.SyncAttributeIndexRequest,
              io.superdurable.gen.SyncAttributeIndexResponse>(
                service, METHODID_SYNC_ATTRIBUTE_INDEXES)))
        .addMethod(
          getGetFlowSummaryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.GetFlowSummaryRequest,
              io.superdurable.gen.GetFlowSummaryResponse>(
                service, METHODID_GET_FLOW_SUMMARY)))
        .addMethod(
          getGetHistoryEventsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.GetHistoryEventsRequest,
              io.superdurable.gen.GetHistoryEventsResponse>(
                service, METHODID_GET_HISTORY_EVENTS)))
        .addMethod(
          getWaitForHistoryEventMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.WaitForHistoryEventRequest,
              io.superdurable.gen.WaitForHistoryEventResponse>(
                service, METHODID_WAIT_FOR_HISTORY_EVENT)))
        .addMethod(
          getGetFlowStateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.GetFlowStateRequest,
              io.superdurable.gen.GetFlowStateResponse>(
                service, METHODID_GET_FLOW_STATE)))
        .addMethod(
          getResetFlowMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.ResetFlowRequest,
              io.superdurable.gen.ResetFlowResponse>(
                service, METHODID_RESET_FLOW)))
        .addMethod(
          getInvokeRPCMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.InvokeRPCRequest,
              io.superdurable.gen.InvokeRPCResponse>(
                service, METHODID_INVOKE_RPC)))
        .addMethod(
          getSkipTimerMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.SkipTimerRequest,
              com.google.protobuf.Empty>(
                service, METHODID_SKIP_TIMER)))
        .addMethod(
          getUpdateFlowConfigMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.UpdateFlowConfigRequest,
              com.google.protobuf.Empty>(
                service, METHODID_UPDATE_FLOW_CONFIG)))
        .addMethod(
          getWaitForStepCompletionMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.WaitForStepCompletionRequest,
              io.superdurable.gen.WaitForStepCompletionResponse>(
                service, METHODID_WAIT_FOR_STEP_COMPLETION)))
        .addMethod(
          getWaitForAttributeMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.WaitForAttributeRequest,
              io.superdurable.gen.WaitForAttributeResponse>(
                service, METHODID_WAIT_FOR_ATTRIBUTE)))
        .addMethod(
          getTriggerContinueAsNewMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.TriggerContinueAsNewRequest,
              com.google.protobuf.Empty>(
                service, METHODID_TRIGGER_CONTINUE_AS_NEW)))
        .addMethod(
          getHealthCheckMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.protobuf.Empty,
              io.superdurable.gen.HealthInfo>(
                service, METHODID_HEALTH_CHECK)))
        .build();
  }

  private static abstract class FlowServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FlowServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.superdurable.gen.DexProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FlowService");
    }
  }

  private static final class FlowServiceFileDescriptorSupplier
      extends FlowServiceBaseDescriptorSupplier {
    FlowServiceFileDescriptorSupplier() {}
  }

  private static final class FlowServiceMethodDescriptorSupplier
      extends FlowServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    FlowServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (FlowServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FlowServiceFileDescriptorSupplier())
              .addMethod(getGetServerInfoMethod())
              .addMethod(getStartFlowMethod())
              .addMethod(getPublishToChannelMethod())
              .addMethod(getGetChannelMessagesMethod())
              .addMethod(getDeleteChannelMessageMethod())
              .addMethod(getWriteStreamMethod())
              .addMethod(getReadStreamMethod())
              .addMethod(getListStreamMessagesMethod())
              .addMethod(getStopFlowMethod())
              .addMethod(getGetAttributesMethod())
              .addMethod(getSetAttributesMethod())
              .addMethod(getLoadBlobsMethod())
              .addMethod(getWaitForFlowMethod())
              .addMethod(getSearchFlowsMethod())
              .addMethod(getSyncAttributeIndexesMethod())
              .addMethod(getGetFlowSummaryMethod())
              .addMethod(getGetHistoryEventsMethod())
              .addMethod(getWaitForHistoryEventMethod())
              .addMethod(getGetFlowStateMethod())
              .addMethod(getResetFlowMethod())
              .addMethod(getInvokeRPCMethod())
              .addMethod(getSkipTimerMethod())
              .addMethod(getUpdateFlowConfigMethod())
              .addMethod(getWaitForStepCompletionMethod())
              .addMethod(getWaitForAttributeMethod())
              .addMethod(getTriggerContinueAsNewMethod())
              .addMethod(getHealthCheckMethod())
              .build();
        }
      }
    }
    return result;
  }
}
