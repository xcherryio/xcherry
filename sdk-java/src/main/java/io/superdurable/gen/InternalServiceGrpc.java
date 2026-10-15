package io.superdurable.gen;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.1)",
    comments = "Source: dex.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class InternalServiceGrpc {

  private InternalServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "dex.InternalService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.ContinueAsNewDumpRequest,
      io.superdurable.gen.ContinueAsNewDumpResponse> getDumpFlowForContinueAsNewMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DumpFlowForContinueAsNew",
      requestType = io.superdurable.gen.ContinueAsNewDumpRequest.class,
      responseType = io.superdurable.gen.ContinueAsNewDumpResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.ContinueAsNewDumpRequest,
      io.superdurable.gen.ContinueAsNewDumpResponse> getDumpFlowForContinueAsNewMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.ContinueAsNewDumpRequest, io.superdurable.gen.ContinueAsNewDumpResponse> getDumpFlowForContinueAsNewMethod;
    if ((getDumpFlowForContinueAsNewMethod = InternalServiceGrpc.getDumpFlowForContinueAsNewMethod) == null) {
      synchronized (InternalServiceGrpc.class) {
        if ((getDumpFlowForContinueAsNewMethod = InternalServiceGrpc.getDumpFlowForContinueAsNewMethod) == null) {
          InternalServiceGrpc.getDumpFlowForContinueAsNewMethod = getDumpFlowForContinueAsNewMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.ContinueAsNewDumpRequest, io.superdurable.gen.ContinueAsNewDumpResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DumpFlowForContinueAsNew"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.ContinueAsNewDumpRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.ContinueAsNewDumpResponse.getDefaultInstance()))
              .setSchemaDescriptor(new InternalServiceMethodDescriptorSupplier("DumpFlowForContinueAsNew"))
              .build();
        }
      }
    }
    return getDumpFlowForContinueAsNewMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static InternalServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<InternalServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<InternalServiceStub>() {
        @java.lang.Override
        public InternalServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new InternalServiceStub(channel, callOptions);
        }
      };
    return InternalServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static InternalServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<InternalServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<InternalServiceBlockingStub>() {
        @java.lang.Override
        public InternalServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new InternalServiceBlockingStub(channel, callOptions);
        }
      };
    return InternalServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static InternalServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<InternalServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<InternalServiceFutureStub>() {
        @java.lang.Override
        public InternalServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new InternalServiceFutureStub(channel, callOptions);
        }
      };
    return InternalServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * </pre>
   */
  public interface AsyncService {

    /**
     */
    default void dumpFlowForContinueAsNew(io.superdurable.gen.ContinueAsNewDumpRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ContinueAsNewDumpResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDumpFlowForContinueAsNewMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service InternalService.
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * </pre>
   */
  public static abstract class InternalServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return InternalServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service InternalService.
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * </pre>
   */
  public static final class InternalServiceStub
      extends io.grpc.stub.AbstractAsyncStub<InternalServiceStub> {
    private InternalServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected InternalServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new InternalServiceStub(channel, callOptions);
    }

    /**
     */
    public void dumpFlowForContinueAsNew(io.superdurable.gen.ContinueAsNewDumpRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.ContinueAsNewDumpResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDumpFlowForContinueAsNewMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service InternalService.
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * </pre>
   */
  public static final class InternalServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<InternalServiceBlockingStub> {
    private InternalServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected InternalServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new InternalServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.superdurable.gen.ContinueAsNewDumpResponse dumpFlowForContinueAsNew(io.superdurable.gen.ContinueAsNewDumpRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDumpFlowForContinueAsNewMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service InternalService.
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * </pre>
   */
  public static final class InternalServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<InternalServiceFutureStub> {
    private InternalServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected InternalServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new InternalServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.ContinueAsNewDumpResponse> dumpFlowForContinueAsNew(
        io.superdurable.gen.ContinueAsNewDumpRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDumpFlowForContinueAsNewMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DUMP_FLOW_FOR_CONTINUE_AS_NEW = 0;

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
        case METHODID_DUMP_FLOW_FOR_CONTINUE_AS_NEW:
          serviceImpl.dumpFlowForContinueAsNew((io.superdurable.gen.ContinueAsNewDumpRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.ContinueAsNewDumpResponse>) responseObserver);
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
          getDumpFlowForContinueAsNewMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.ContinueAsNewDumpRequest,
              io.superdurable.gen.ContinueAsNewDumpResponse>(
                service, METHODID_DUMP_FLOW_FOR_CONTINUE_AS_NEW)))
        .build();
  }

  private static abstract class InternalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    InternalServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.superdurable.gen.DexProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("InternalService");
    }
  }

  private static final class InternalServiceFileDescriptorSupplier
      extends InternalServiceBaseDescriptorSupplier {
    InternalServiceFileDescriptorSupplier() {}
  }

  private static final class InternalServiceMethodDescriptorSupplier
      extends InternalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    InternalServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (InternalServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new InternalServiceFileDescriptorSupplier())
              .addMethod(getDumpFlowForContinueAsNewMethod())
              .build();
        }
      }
    }
    return result;
  }
}
