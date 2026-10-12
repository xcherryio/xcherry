package io.superdurable.gen;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
 * TenantAdminService manages the tenants of a multi-tenant server. Only operator principals may call it.
 * Every RPC names its tenant explicitly and never returns Flow data.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.1)",
    comments = "Source: dex.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class TenantAdminServiceGrpc {

  private TenantAdminServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "dex.TenantAdminService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.TombstoneTenantRequest,
      io.superdurable.gen.TombstoneTenantResponse> getTombstoneTenantMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TombstoneTenant",
      requestType = io.superdurable.gen.TombstoneTenantRequest.class,
      responseType = io.superdurable.gen.TombstoneTenantResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.TombstoneTenantRequest,
      io.superdurable.gen.TombstoneTenantResponse> getTombstoneTenantMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.TombstoneTenantRequest, io.superdurable.gen.TombstoneTenantResponse> getTombstoneTenantMethod;
    if ((getTombstoneTenantMethod = TenantAdminServiceGrpc.getTombstoneTenantMethod) == null) {
      synchronized (TenantAdminServiceGrpc.class) {
        if ((getTombstoneTenantMethod = TenantAdminServiceGrpc.getTombstoneTenantMethod) == null) {
          TenantAdminServiceGrpc.getTombstoneTenantMethod = getTombstoneTenantMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.TombstoneTenantRequest, io.superdurable.gen.TombstoneTenantResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TombstoneTenant"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.TombstoneTenantRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.TombstoneTenantResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TenantAdminServiceMethodDescriptorSupplier("TombstoneTenant"))
              .build();
        }
      }
    }
    return getTombstoneTenantMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.PurgeTenantRequest,
      io.superdurable.gen.PurgeTenantResponse> getPurgeTenantMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PurgeTenant",
      requestType = io.superdurable.gen.PurgeTenantRequest.class,
      responseType = io.superdurable.gen.PurgeTenantResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.PurgeTenantRequest,
      io.superdurable.gen.PurgeTenantResponse> getPurgeTenantMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.PurgeTenantRequest, io.superdurable.gen.PurgeTenantResponse> getPurgeTenantMethod;
    if ((getPurgeTenantMethod = TenantAdminServiceGrpc.getPurgeTenantMethod) == null) {
      synchronized (TenantAdminServiceGrpc.class) {
        if ((getPurgeTenantMethod = TenantAdminServiceGrpc.getPurgeTenantMethod) == null) {
          TenantAdminServiceGrpc.getPurgeTenantMethod = getPurgeTenantMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.PurgeTenantRequest, io.superdurable.gen.PurgeTenantResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PurgeTenant"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.PurgeTenantRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.PurgeTenantResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TenantAdminServiceMethodDescriptorSupplier("PurgeTenant"))
              .build();
        }
      }
    }
    return getPurgeTenantMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.superdurable.gen.DescribeTenantRequest,
      io.superdurable.gen.DescribeTenantResponse> getDescribeTenantMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DescribeTenant",
      requestType = io.superdurable.gen.DescribeTenantRequest.class,
      responseType = io.superdurable.gen.DescribeTenantResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.superdurable.gen.DescribeTenantRequest,
      io.superdurable.gen.DescribeTenantResponse> getDescribeTenantMethod() {
    io.grpc.MethodDescriptor<io.superdurable.gen.DescribeTenantRequest, io.superdurable.gen.DescribeTenantResponse> getDescribeTenantMethod;
    if ((getDescribeTenantMethod = TenantAdminServiceGrpc.getDescribeTenantMethod) == null) {
      synchronized (TenantAdminServiceGrpc.class) {
        if ((getDescribeTenantMethod = TenantAdminServiceGrpc.getDescribeTenantMethod) == null) {
          TenantAdminServiceGrpc.getDescribeTenantMethod = getDescribeTenantMethod =
              io.grpc.MethodDescriptor.<io.superdurable.gen.DescribeTenantRequest, io.superdurable.gen.DescribeTenantResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DescribeTenant"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.DescribeTenantRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.superdurable.gen.DescribeTenantResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TenantAdminServiceMethodDescriptorSupplier("DescribeTenant"))
              .build();
        }
      }
    }
    return getDescribeTenantMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TenantAdminServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TenantAdminServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TenantAdminServiceStub>() {
        @java.lang.Override
        public TenantAdminServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TenantAdminServiceStub(channel, callOptions);
        }
      };
    return TenantAdminServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TenantAdminServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TenantAdminServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TenantAdminServiceBlockingStub>() {
        @java.lang.Override
        public TenantAdminServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TenantAdminServiceBlockingStub(channel, callOptions);
        }
      };
    return TenantAdminServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TenantAdminServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TenantAdminServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TenantAdminServiceFutureStub>() {
        @java.lang.Override
        public TenantAdminServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TenantAdminServiceFutureStub(channel, callOptions);
        }
      };
    return TenantAdminServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * TenantAdminService manages the tenants of a multi-tenant server. Only operator principals may call it.
   * Every RPC names its tenant explicitly and never returns Flow data.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * Refuses every credential of the tenant within the tombstone refresh interval. Idempotent.
     * </pre>
     */
    default void tombstoneTenant(io.superdurable.gen.TombstoneTenantRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.TombstoneTenantResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTombstoneTenantMethod(), responseObserver);
    }

    /**
     * <pre>
     * Deletes one bounded batch of the tombstoned tenant's data. Repeat until done is true.
     * </pre>
     */
    default void purgeTenant(io.superdurable.gen.PurgeTenantRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.PurgeTenantResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPurgeTenantMethod(), responseObserver);
    }

    /**
     */
    default void describeTenant(io.superdurable.gen.DescribeTenantRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.DescribeTenantResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDescribeTenantMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service TenantAdminService.
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * TenantAdminService manages the tenants of a multi-tenant server. Only operator principals may call it.
   * Every RPC names its tenant explicitly and never returns Flow data.
   * </pre>
   */
  public static abstract class TenantAdminServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return TenantAdminServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service TenantAdminService.
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * TenantAdminService manages the tenants of a multi-tenant server. Only operator principals may call it.
   * Every RPC names its tenant explicitly and never returns Flow data.
   * </pre>
   */
  public static final class TenantAdminServiceStub
      extends io.grpc.stub.AbstractAsyncStub<TenantAdminServiceStub> {
    private TenantAdminServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TenantAdminServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TenantAdminServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Refuses every credential of the tenant within the tombstone refresh interval. Idempotent.
     * </pre>
     */
    public void tombstoneTenant(io.superdurable.gen.TombstoneTenantRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.TombstoneTenantResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTombstoneTenantMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Deletes one bounded batch of the tombstoned tenant's data. Repeat until done is true.
     * </pre>
     */
    public void purgeTenant(io.superdurable.gen.PurgeTenantRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.PurgeTenantResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPurgeTenantMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void describeTenant(io.superdurable.gen.DescribeTenantRequest request,
        io.grpc.stub.StreamObserver<io.superdurable.gen.DescribeTenantResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDescribeTenantMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service TenantAdminService.
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * TenantAdminService manages the tenants of a multi-tenant server. Only operator principals may call it.
   * Every RPC names its tenant explicitly and never returns Flow data.
   * </pre>
   */
  public static final class TenantAdminServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<TenantAdminServiceBlockingStub> {
    private TenantAdminServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TenantAdminServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TenantAdminServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Refuses every credential of the tenant within the tombstone refresh interval. Idempotent.
     * </pre>
     */
    public io.superdurable.gen.TombstoneTenantResponse tombstoneTenant(io.superdurable.gen.TombstoneTenantRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTombstoneTenantMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Deletes one bounded batch of the tombstoned tenant's data. Repeat until done is true.
     * </pre>
     */
    public io.superdurable.gen.PurgeTenantResponse purgeTenant(io.superdurable.gen.PurgeTenantRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPurgeTenantMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.superdurable.gen.DescribeTenantResponse describeTenant(io.superdurable.gen.DescribeTenantRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDescribeTenantMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service TenantAdminService.
   * <pre>
   * Server-internal only (interpreter CAN activity → API). Not SDK-facing.
   * TenantAdminService manages the tenants of a multi-tenant server. Only operator principals may call it.
   * Every RPC names its tenant explicitly and never returns Flow data.
   * </pre>
   */
  public static final class TenantAdminServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<TenantAdminServiceFutureStub> {
    private TenantAdminServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TenantAdminServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TenantAdminServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Refuses every credential of the tenant within the tombstone refresh interval. Idempotent.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.TombstoneTenantResponse> tombstoneTenant(
        io.superdurable.gen.TombstoneTenantRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTombstoneTenantMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Deletes one bounded batch of the tombstoned tenant's data. Repeat until done is true.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.PurgeTenantResponse> purgeTenant(
        io.superdurable.gen.PurgeTenantRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPurgeTenantMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.superdurable.gen.DescribeTenantResponse> describeTenant(
        io.superdurable.gen.DescribeTenantRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDescribeTenantMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_TOMBSTONE_TENANT = 0;
  private static final int METHODID_PURGE_TENANT = 1;
  private static final int METHODID_DESCRIBE_TENANT = 2;

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
        case METHODID_TOMBSTONE_TENANT:
          serviceImpl.tombstoneTenant((io.superdurable.gen.TombstoneTenantRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.TombstoneTenantResponse>) responseObserver);
          break;
        case METHODID_PURGE_TENANT:
          serviceImpl.purgeTenant((io.superdurable.gen.PurgeTenantRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.PurgeTenantResponse>) responseObserver);
          break;
        case METHODID_DESCRIBE_TENANT:
          serviceImpl.describeTenant((io.superdurable.gen.DescribeTenantRequest) request,
              (io.grpc.stub.StreamObserver<io.superdurable.gen.DescribeTenantResponse>) responseObserver);
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
          getTombstoneTenantMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.TombstoneTenantRequest,
              io.superdurable.gen.TombstoneTenantResponse>(
                service, METHODID_TOMBSTONE_TENANT)))
        .addMethod(
          getPurgeTenantMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.PurgeTenantRequest,
              io.superdurable.gen.PurgeTenantResponse>(
                service, METHODID_PURGE_TENANT)))
        .addMethod(
          getDescribeTenantMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.superdurable.gen.DescribeTenantRequest,
              io.superdurable.gen.DescribeTenantResponse>(
                service, METHODID_DESCRIBE_TENANT)))
        .build();
  }

  private static abstract class TenantAdminServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TenantAdminServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.superdurable.gen.DexProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TenantAdminService");
    }
  }

  private static final class TenantAdminServiceFileDescriptorSupplier
      extends TenantAdminServiceBaseDescriptorSupplier {
    TenantAdminServiceFileDescriptorSupplier() {}
  }

  private static final class TenantAdminServiceMethodDescriptorSupplier
      extends TenantAdminServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    TenantAdminServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (TenantAdminServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TenantAdminServiceFileDescriptorSupplier())
              .addMethod(getTombstoneTenantMethod())
              .addMethod(getPurgeTenantMethod())
              .addMethod(getDescribeTenantMethod())
              .build();
        }
      }
    }
    return result;
  }
}
