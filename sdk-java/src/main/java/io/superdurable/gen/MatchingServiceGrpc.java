package io.superdurable.gen;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Hosted on the internal listener; server instances route tasks and Worker streams to queue owners.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.1)",
    comments = "Source: dex.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class MatchingServiceGrpc {

  private MatchingServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "dex.MatchingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.DispatchTaskRequest,
      io.superdurable.gen.DispatchTaskResponse> getDispatchTaskMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DispatchTask",
      requestType = io.superdurable.gen.DispatchTaskRequest.class,
      responseType = io.superdurable.gen.DispatchTaskResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.DispatchTaskRequest,
      io.superdurable.gen.DispatchTaskResponse> getDispatchTaskMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.DispatchTaskRequest, io.superdurable.gen.DispatchTaskResponse> getDispatchTaskMethod;
    if ((getDispatchTaskMethod = MatchingServiceGrpc.getDispatchTaskMethod) == null) {
      synchronized (MatchingServiceGrpc.class) {
        if ((getDispatchTaskMethod = MatchingServiceGrpc.getDispatchTaskMethod) == null) {
          MatchingServiceGrpc.getDispatchTaskMethod = getDispatchTaskMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.DispatchTaskRequest, io.superdurable.gen.DispatchTaskResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DispatchTask"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.DispatchTaskRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.DispatchTaskResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MatchingServiceMethodDescriptorSupplier("DispatchTask"))
              .build();
        }
      }
    }
    return getDispatchTaskMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.PollWorkerTaskRequest,
      io.superdurable.gen.PollWorkerTaskResponse> getForwardPollWorkerTaskMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ForwardPollWorkerTask",
      requestType = io.superdurable.gen.PollWorkerTaskRequest.class,
      responseType = io.superdurable.gen.PollWorkerTaskResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.PollWorkerTaskRequest,
      io.superdurable.gen.PollWorkerTaskResponse> getForwardPollWorkerTaskMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.PollWorkerTaskRequest, io.superdurable.gen.PollWorkerTaskResponse> getForwardPollWorkerTaskMethod;
    if ((getForwardPollWorkerTaskMethod = MatchingServiceGrpc.getForwardPollWorkerTaskMethod) == null) {
      synchronized (MatchingServiceGrpc.class) {
        if ((getForwardPollWorkerTaskMethod = MatchingServiceGrpc.getForwardPollWorkerTaskMethod) == null) {
          MatchingServiceGrpc.getForwardPollWorkerTaskMethod = getForwardPollWorkerTaskMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.PollWorkerTaskRequest, io.superdurable.gen.PollWorkerTaskResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ForwardPollWorkerTask"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.PollWorkerTaskRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.PollWorkerTaskResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MatchingServiceMethodDescriptorSupplier("ForwardPollWorkerTask"))
              .build();
        }
      }
    }
    return getForwardPollWorkerTaskMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MatchingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MatchingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MatchingServiceStub>() {
        @java.lang.Override
        public MatchingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MatchingServiceStub(channel, callOptions);
        }
      };
    return MatchingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MatchingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MatchingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MatchingServiceBlockingStub>() {
        @java.lang.Override
        public MatchingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MatchingServiceBlockingStub(channel, callOptions);
        }
      };
    return MatchingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MatchingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MatchingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MatchingServiceFutureStub>() {
        @java.lang.Override
        public MatchingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MatchingServiceFutureStub(channel, callOptions);
        }
      };
    return MatchingServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Hosted on the internal listener; server instances route tasks and Worker streams to queue owners.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Streams Worker output frames and a final result or dispatch error.
     * </pre>
     */
    default void dispatchTask(io.superdurable.gen.DispatchTaskRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.DispatchTaskResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDispatchTaskMethod(), responseObserver);
    }

    /**
     * <pre>
     * Proxies a Worker's queue stream from its receiving instance to the queue owner.
     * </pre>
     */
    default io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskRequest> forwardPollWorkerTask(
        io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getForwardPollWorkerTaskMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service MatchingService.
   * <pre>
   * Hosted on the internal listener; server instances route tasks and Worker streams to queue owners.
   * </pre>
   */
  public static abstract class MatchingServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return MatchingServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service MatchingService.
   * <pre>
   * Hosted on the internal listener; server instances route tasks and Worker streams to queue owners.
   * </pre>
   */
  public static final class MatchingServiceStub
      extends io.grpc.stub.AbstractAsyncStub<MatchingServiceStub> {
    private MatchingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MatchingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MatchingServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Streams Worker output frames and a final result or dispatch error.
     * </pre>
     */
    public void dispatchTask(io.superdurable.gen.DispatchTaskRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.DispatchTaskResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getDispatchTaskMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Proxies a Worker's queue stream from its receiving instance to the queue owner.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskRequest> forwardPollWorkerTask(
        io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getForwardPollWorkerTaskMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service MatchingService.
   * <pre>
   * Hosted on the internal listener; server instances route tasks and Worker streams to queue owners.
   * </pre>
   */
  public static final class MatchingServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<MatchingServiceBlockingStub> {
    private MatchingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MatchingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MatchingServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Streams Worker output frames and a final result or dispatch error.
     * </pre>
     */
    public java.util.Iterator<io.superdurable.gen.DispatchTaskResponse> dispatchTask(
        io.superdurable.gen.DispatchTaskRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getDispatchTaskMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service MatchingService.
   * <pre>
   * Hosted on the internal listener; server instances route tasks and Worker streams to queue owners.
   * </pre>
   */
  public static final class MatchingServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<MatchingServiceFutureStub> {
    private MatchingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MatchingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MatchingServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_DISPATCH_TASK = 0;
  private static final int METHODID_FORWARD_POLL_WORKER_TASK = 1;

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
        case METHODID_DISPATCH_TASK:
          serviceImpl.dispatchTask((io.superdurable.gen.DispatchTaskRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.DispatchTaskResponse>) responseObserver);
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
        case METHODID_FORWARD_POLL_WORKER_TASK:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.forwardPollWorkerTask(
              (io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getDispatchTaskMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              io.superdurable.gen.DispatchTaskRequest,
              io.superdurable.gen.DispatchTaskResponse>(
                service, METHODID_DISPATCH_TASK)))
        .addMethod(
          getForwardPollWorkerTaskMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              io.superdurable.gen.PollWorkerTaskRequest,
              io.superdurable.gen.PollWorkerTaskResponse>(
                service, METHODID_FORWARD_POLL_WORKER_TASK)))
        .build();
  }

  private static abstract class MatchingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MatchingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.superdurable.gen.DexProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MatchingService");
    }
  }

  private static final class MatchingServiceFileDescriptorSupplier
      extends MatchingServiceBaseDescriptorSupplier {
    MatchingServiceFileDescriptorSupplier() {}
  }

  private static final class MatchingServiceMethodDescriptorSupplier
      extends MatchingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    MatchingServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (MatchingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MatchingServiceFileDescriptorSupplier())
              .addMethod(getDispatchTaskMethod())
              .addMethod(getForwardPollWorkerTaskMethod())
              .build();
        }
      }
    }
    return result;
  }
}
