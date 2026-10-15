package io.superdurable.gen;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Hosted by Dex server; pull Workers open one queue stream for at most one task at a time.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.1)",
    comments = "Source: dex.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class WorkerTaskServiceGrpc {

  private WorkerTaskServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "dex.WorkerTaskService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.PollWorkerTaskRequest,
      io.superdurable.gen.PollWorkerTaskResponse> getPollWorkerTaskMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PollWorkerTask",
      requestType = io.superdurable.gen.PollWorkerTaskRequest.class,
      responseType = io.superdurable.gen.PollWorkerTaskResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.PollWorkerTaskRequest,
      io.superdurable.gen.PollWorkerTaskResponse> getPollWorkerTaskMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.PollWorkerTaskRequest, io.superdurable.gen.PollWorkerTaskResponse> getPollWorkerTaskMethod;
    if ((getPollWorkerTaskMethod = WorkerTaskServiceGrpc.getPollWorkerTaskMethod) == null) {
      synchronized (WorkerTaskServiceGrpc.class) {
        if ((getPollWorkerTaskMethod = WorkerTaskServiceGrpc.getPollWorkerTaskMethod) == null) {
          WorkerTaskServiceGrpc.getPollWorkerTaskMethod = getPollWorkerTaskMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.PollWorkerTaskRequest, io.superdurable.gen.PollWorkerTaskResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PollWorkerTask"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.PollWorkerTaskRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.PollWorkerTaskResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WorkerTaskServiceMethodDescriptorSupplier("PollWorkerTask"))
              .build();
        }
      }
    }
    return getPollWorkerTaskMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static WorkerTaskServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkerTaskServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkerTaskServiceStub>() {
        @java.lang.Override
        public WorkerTaskServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkerTaskServiceStub(channel, callOptions);
        }
      };
    return WorkerTaskServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static WorkerTaskServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkerTaskServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkerTaskServiceBlockingStub>() {
        @java.lang.Override
        public WorkerTaskServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkerTaskServiceBlockingStub(channel, callOptions);
        }
      };
    return WorkerTaskServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static WorkerTaskServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkerTaskServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkerTaskServiceFutureStub>() {
        @java.lang.Override
        public WorkerTaskServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkerTaskServiceFutureStub(channel, callOptions);
        }
      };
    return WorkerTaskServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Hosted by Dex server; pull Workers open one queue stream for at most one task at a time.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Exchanges registration, task, result, and control frames for one Dex task queue.
     * </pre>
     */
    default io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskRequest> pollWorkerTask(
        io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getPollWorkerTaskMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service WorkerTaskService.
   * <pre>
   * Hosted by Dex server; pull Workers open one queue stream for at most one task at a time.
   * </pre>
   */
  public static abstract class WorkerTaskServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return WorkerTaskServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service WorkerTaskService.
   * <pre>
   * Hosted by Dex server; pull Workers open one queue stream for at most one task at a time.
   * </pre>
   */
  public static final class WorkerTaskServiceStub
      extends io.grpc.stub.AbstractAsyncStub<WorkerTaskServiceStub> {
    private WorkerTaskServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkerTaskServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkerTaskServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Exchanges registration, task, result, and control frames for one Dex task queue.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskRequest> pollWorkerTask(
        io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getPollWorkerTaskMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service WorkerTaskService.
   * <pre>
   * Hosted by Dex server; pull Workers open one queue stream for at most one task at a time.
   * </pre>
   */
  public static final class WorkerTaskServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<WorkerTaskServiceBlockingStub> {
    private WorkerTaskServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkerTaskServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkerTaskServiceBlockingStub(channel, callOptions);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service WorkerTaskService.
   * <pre>
   * Hosted by Dex server; pull Workers open one queue stream for at most one task at a time.
   * </pre>
   */
  public static final class WorkerTaskServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<WorkerTaskServiceFutureStub> {
    private WorkerTaskServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkerTaskServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkerTaskServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_POLL_WORKER_TASK = 0;

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
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_POLL_WORKER_TASK:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.pollWorkerTask(
              (io.grpc.stub.StreamObserver<io.superdurable.gen.PollWorkerTaskResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getPollWorkerTaskMethod(),
          io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
            new MethodHandlers<
              io.superdurable.gen.PollWorkerTaskRequest,
              io.superdurable.gen.PollWorkerTaskResponse>(
                service, METHODID_POLL_WORKER_TASK)))
        .build();
  }

  private static abstract class WorkerTaskServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    WorkerTaskServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.superdurable.gen.DexProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("WorkerTaskService");
    }
  }

  private static final class WorkerTaskServiceFileDescriptorSupplier
      extends WorkerTaskServiceBaseDescriptorSupplier {
    WorkerTaskServiceFileDescriptorSupplier() {}
  }

  private static final class WorkerTaskServiceMethodDescriptorSupplier
      extends WorkerTaskServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    WorkerTaskServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (WorkerTaskServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new WorkerTaskServiceFileDescriptorSupplier())
              .addMethod(getPollWorkerTaskMethod())
              .build();
        }
      }
    }
    return result;
  }
}
