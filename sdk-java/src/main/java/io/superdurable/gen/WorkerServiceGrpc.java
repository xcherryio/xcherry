package io.superdurable.gen;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Hosted by Dex worker; server calls these RPCs.
 * server-zd supports only pull Workers and never calls WorkerService.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.1)",
    comments = "Source: dex.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class WorkerServiceGrpc {

  private WorkerServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "dex.WorkerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.InvokeWaitForMethodRequest,
      io.superdurable.gen.InvokeWaitForMethodOutput> getInvokeWaitForMethodMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "InvokeWaitForMethod",
      requestType = io.superdurable.gen.InvokeWaitForMethodRequest.class,
      responseType = io.superdurable.gen.InvokeWaitForMethodOutput.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.InvokeWaitForMethodRequest,
      io.superdurable.gen.InvokeWaitForMethodOutput> getInvokeWaitForMethodMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.InvokeWaitForMethodRequest, io.superdurable.gen.InvokeWaitForMethodOutput> getInvokeWaitForMethodMethod;
    if ((getInvokeWaitForMethodMethod = WorkerServiceGrpc.getInvokeWaitForMethodMethod) == null) {
      synchronized (WorkerServiceGrpc.class) {
        if ((getInvokeWaitForMethodMethod = WorkerServiceGrpc.getInvokeWaitForMethodMethod) == null) {
          WorkerServiceGrpc.getInvokeWaitForMethodMethod = getInvokeWaitForMethodMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.InvokeWaitForMethodRequest, io.superdurable.gen.InvokeWaitForMethodOutput>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "InvokeWaitForMethod"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.InvokeWaitForMethodRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.InvokeWaitForMethodOutput.getDefaultInstance()))
              .setSchemaDescriptor(new WorkerServiceMethodDescriptorSupplier("InvokeWaitForMethod"))
              .build();
        }
      }
    }
    return getInvokeWaitForMethodMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.InvokeExecuteMethodRequest,
      io.superdurable.gen.InvokeExecuteMethodOutput> getInvokeExecuteMethodMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "InvokeExecuteMethod",
      requestType = io.superdurable.gen.InvokeExecuteMethodRequest.class,
      responseType = io.superdurable.gen.InvokeExecuteMethodOutput.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.InvokeExecuteMethodRequest,
      io.superdurable.gen.InvokeExecuteMethodOutput> getInvokeExecuteMethodMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.InvokeExecuteMethodRequest, io.superdurable.gen.InvokeExecuteMethodOutput> getInvokeExecuteMethodMethod;
    if ((getInvokeExecuteMethodMethod = WorkerServiceGrpc.getInvokeExecuteMethodMethod) == null) {
      synchronized (WorkerServiceGrpc.class) {
        if ((getInvokeExecuteMethodMethod = WorkerServiceGrpc.getInvokeExecuteMethodMethod) == null) {
          WorkerServiceGrpc.getInvokeExecuteMethodMethod = getInvokeExecuteMethodMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.InvokeExecuteMethodRequest, io.superdurable.gen.InvokeExecuteMethodOutput>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "InvokeExecuteMethod"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.InvokeExecuteMethodRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.InvokeExecuteMethodOutput.getDefaultInstance()))
              .setSchemaDescriptor(new WorkerServiceMethodDescriptorSupplier("InvokeExecuteMethod"))
              .build();
        }
      }
    }
    return getInvokeExecuteMethodMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.InvokeWorkerRPCRequest,
      io.superdurable.gen.InvokeWorkerRPCResponse> getInvokeWorkerRPCMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "InvokeWorkerRPC",
      requestType = io.superdurable.gen.InvokeWorkerRPCRequest.class,
      responseType = io.superdurable.gen.InvokeWorkerRPCResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.InvokeWorkerRPCRequest,
      io.superdurable.gen.InvokeWorkerRPCResponse> getInvokeWorkerRPCMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.InvokeWorkerRPCRequest, io.superdurable.gen.InvokeWorkerRPCResponse> getInvokeWorkerRPCMethod;
    if ((getInvokeWorkerRPCMethod = WorkerServiceGrpc.getInvokeWorkerRPCMethod) == null) {
      synchronized (WorkerServiceGrpc.class) {
        if ((getInvokeWorkerRPCMethod = WorkerServiceGrpc.getInvokeWorkerRPCMethod) == null) {
          WorkerServiceGrpc.getInvokeWorkerRPCMethod = getInvokeWorkerRPCMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.InvokeWorkerRPCRequest, io.superdurable.gen.InvokeWorkerRPCResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "InvokeWorkerRPC"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.InvokeWorkerRPCRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.InvokeWorkerRPCResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WorkerServiceMethodDescriptorSupplier("InvokeWorkerRPC"))
              .build();
        }
      }
    }
    return getInvokeWorkerRPCMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static WorkerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkerServiceStub>() {
        @java.lang.Override
        public WorkerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkerServiceStub(channel, callOptions);
        }
      };
    return WorkerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static WorkerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkerServiceBlockingStub>() {
        @java.lang.Override
        public WorkerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkerServiceBlockingStub(channel, callOptions);
        }
      };
    return WorkerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static WorkerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WorkerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WorkerServiceFutureStub>() {
        @java.lang.Override
        public WorkerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WorkerServiceFutureStub(channel, callOptions);
        }
      };
    return WorkerServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Hosted by Dex worker; server calls these RPCs.
   * server-zd supports only pull Workers and never calls WorkerService.
   * </pre>
   */
  public interface AsyncService {

    /**
     */
    default void invokeWaitForMethod(io.superdurable.gen.InvokeWaitForMethodRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeWaitForMethodOutput> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInvokeWaitForMethodMethod(), responseObserver);
    }

    /**
     */
    default void invokeExecuteMethod(io.superdurable.gen.InvokeExecuteMethodRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeExecuteMethodOutput> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInvokeExecuteMethodMethod(), responseObserver);
    }

    /**
     */
    default void invokeWorkerRPC(io.superdurable.gen.InvokeWorkerRPCRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeWorkerRPCResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInvokeWorkerRPCMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service WorkerService.
   * <pre>
   * Hosted by Dex worker; server calls these RPCs.
   * server-zd supports only pull Workers and never calls WorkerService.
   * </pre>
   */
  public static abstract class WorkerServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return WorkerServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service WorkerService.
   * <pre>
   * Hosted by Dex worker; server calls these RPCs.
   * server-zd supports only pull Workers and never calls WorkerService.
   * </pre>
   */
  public static final class WorkerServiceStub
      extends io.grpc.stub.AbstractAsyncStub<WorkerServiceStub> {
    private WorkerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkerServiceStub(channel, callOptions);
    }

    /**
     */
    public void invokeWaitForMethod(io.superdurable.gen.InvokeWaitForMethodRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeWaitForMethodOutput> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getInvokeWaitForMethodMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void invokeExecuteMethod(io.superdurable.gen.InvokeExecuteMethodRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeExecuteMethodOutput> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getInvokeExecuteMethodMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void invokeWorkerRPC(io.superdurable.gen.InvokeWorkerRPCRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeWorkerRPCResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getInvokeWorkerRPCMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service WorkerService.
   * <pre>
   * Hosted by Dex worker; server calls these RPCs.
   * server-zd supports only pull Workers and never calls WorkerService.
   * </pre>
   */
  public static final class WorkerServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<WorkerServiceBlockingStub> {
    private WorkerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkerServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<io.superdurable.gen.InvokeWaitForMethodOutput> invokeWaitForMethod(
        io.superdurable.gen.InvokeWaitForMethodRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getInvokeWaitForMethodMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<io.superdurable.gen.InvokeExecuteMethodOutput> invokeExecuteMethod(
        io.superdurable.gen.InvokeExecuteMethodRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getInvokeExecuteMethodMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.InvokeWorkerRPCResponse invokeWorkerRPC(io.superdurable.gen.InvokeWorkerRPCRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getInvokeWorkerRPCMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service WorkerService.
   * <pre>
   * Hosted by Dex worker; server calls these RPCs.
   * server-zd supports only pull Workers and never calls WorkerService.
   * </pre>
   */
  public static final class WorkerServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<WorkerServiceFutureStub> {
    private WorkerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WorkerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WorkerServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.InvokeWorkerRPCResponse> invokeWorkerRPC(
        io.superdurable.gen.InvokeWorkerRPCRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getInvokeWorkerRPCMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_INVOKE_WAIT_FOR_METHOD = 0;
  private static final int METHODID_INVOKE_EXECUTE_METHOD = 1;
  private static final int METHODID_INVOKE_WORKER_RPC = 2;

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
        case METHODID_INVOKE_WAIT_FOR_METHOD:
          serviceImpl.invokeWaitForMethod((io.superdurable.gen.InvokeWaitForMethodRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeWaitForMethodOutput>) responseObserver);
          break;
        case METHODID_INVOKE_EXECUTE_METHOD:
          serviceImpl.invokeExecuteMethod((io.superdurable.gen.InvokeExecuteMethodRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeExecuteMethodOutput>) responseObserver);
          break;
        case METHODID_INVOKE_WORKER_RPC:
          serviceImpl.invokeWorkerRPC((io.superdurable.gen.InvokeWorkerRPCRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.InvokeWorkerRPCResponse>) responseObserver);
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
          getInvokeWaitForMethodMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              io.superdurable.gen.InvokeWaitForMethodRequest,
              io.superdurable.gen.InvokeWaitForMethodOutput>(
                service, METHODID_INVOKE_WAIT_FOR_METHOD)))
        .addMethod(
          getInvokeExecuteMethodMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              io.superdurable.gen.InvokeExecuteMethodRequest,
              io.superdurable.gen.InvokeExecuteMethodOutput>(
                service, METHODID_INVOKE_EXECUTE_METHOD)))
        .addMethod(
          getInvokeWorkerRPCMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.InvokeWorkerRPCRequest,
              io.superdurable.gen.InvokeWorkerRPCResponse>(
                service, METHODID_INVOKE_WORKER_RPC)))
        .build();
  }

  private static abstract class WorkerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    WorkerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.superdurable.gen.DexProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("WorkerService");
    }
  }

  private static final class WorkerServiceFileDescriptorSupplier
      extends WorkerServiceBaseDescriptorSupplier {
    WorkerServiceFileDescriptorSupplier() {}
  }

  private static final class WorkerServiceMethodDescriptorSupplier
      extends WorkerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    WorkerServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (WorkerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new WorkerServiceFileDescriptorSupplier())
              .addMethod(getInvokeWaitForMethodMethod())
              .addMethod(getInvokeExecuteMethodMethod())
              .addMethod(getInvokeWorkerRPCMethod())
              .build();
        }
      }
    }
    return result;
  }
}
