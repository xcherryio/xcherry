// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Java JNI bindings for the shared Dex blob cache.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use dex_blob_cache::{BlobCache, BlobCacheConfig};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jbyteArray, jlong};

#[expect(
    unsafe_code,
    reason = "JNI requires an unmangled exported symbol for the Java native method."
)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_superdurable_dex_NativeBlobCacheBridge_cacheOpen(
    mut env: JNIEnv,
    _class: JClass,
    directory: JString,
    max_bytes: jlong,
    frequency_counters: jlong,
) -> jlong {
    let result = catch_result(|| {
        let directory: String = env
            .get_string(&directory)
            .map_err(|error| error.to_string())?
            .into();
        let config = BlobCacheConfig::new(directory, max_bytes, frequency_counters)
            .map_err(|error| error.to_string())?;
        BlobCache::open(config)
            .map(|cache| Box::into_raw(Box::new(cache)) as jlong)
            .map_err(|error| error.to_string())
    });
    jni_result(&mut env, result, 0)
}

#[expect(
    unsafe_code,
    reason = "JNI requires an unmangled exported symbol for the Java native method."
)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_superdurable_dex_NativeBlobCacheBridge_cacheGet(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    blob_id: JString,
) -> jbyteArray {
    let result = catch_result(|| {
        let cache = native_cache(handle)?;
        let blob_id: String = env
            .get_string(&blob_id)
            .map_err(|error| error.to_string())?
            .into();
        cache
            .get(&blob_id)
            .map_err(|error| error.to_string())?
            .map(|payload| {
                env.byte_array_from_slice(&payload)
                    .map(|array| array.into_raw())
                    .map_err(|error| error.to_string())
            })
            .transpose()
    });
    match result {
        Ok(Some(payload)) => payload,
        Ok(None) => std::ptr::null_mut(),
        Err(message) => {
            throw_illegal_state(&mut env, message);
            std::ptr::null_mut()
        }
    }
}

#[expect(
    unsafe_code,
    reason = "JNI requires an unmangled exported symbol for the Java native method."
)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_superdurable_dex_NativeBlobCacheBridge_cachePut(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    blob_id: JString,
    payload: JByteArray,
) -> jboolean {
    let result = catch_result(|| {
        let cache = native_cache(handle)?;
        let blob_id: String = env
            .get_string(&blob_id)
            .map_err(|error| error.to_string())?
            .into();
        let payload = env
            .convert_byte_array(&payload)
            .map_err(|error| error.to_string())?;
        cache
            .put(&blob_id, &payload)
            .map_err(|error| error.to_string())
    });
    if jni_result(&mut env, result, false) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[expect(
    unsafe_code,
    reason = "JNI requires an unmangled exported symbol for the Java native method."
)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_superdurable_dex_NativeBlobCacheBridge_cacheDelete(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    blob_id: JString,
) {
    let result = catch_result(|| {
        let cache = native_cache(handle)?;
        let blob_id: String = env
            .get_string(&blob_id)
            .map_err(|error| error.to_string())?
            .into();
        cache.delete(&blob_id).map_err(|error| error.to_string())
    });
    jni_result(&mut env, result, ());
}

#[expect(
    unsafe_code,
    reason = "JNI requires an unmangled exported symbol for the Java native method."
)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_superdurable_dex_NativeBlobCacheBridge_cacheDeleteAll(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    let result = catch_result(|| {
        native_cache(handle)?
            .delete_all()
            .map_err(|error| error.to_string())
    });
    jni_result(&mut env, result, ());
}

#[expect(
    unsafe_code,
    reason = "JNI requires an unmangled exported symbol for the Java native method."
)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_io_superdurable_dex_NativeBlobCacheBridge_cacheClose(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    let result = catch_result(|| {
        if handle == 0 {
            return Err("native BlobCache handle is closed".to_owned());
        }
        // SAFETY: cacheOpen returns this Box exactly once; Java clears the handle before close.
        #[expect(
            unsafe_code,
            reason = "Java transfers the unique cache allocation back during close."
        )]
        let cache = unsafe { Box::from_raw(handle as *mut BlobCache) };
        cache.close().map_err(|error| error.to_string())
    });
    jni_result(&mut env, result, ());
}

fn native_cache(handle: jlong) -> Result<&'static BlobCache, String> {
    if handle == 0 {
        return Err("native BlobCache handle is closed".to_owned());
    }
    // SAFETY: Java's lifecycle lock prevents cacheClose while an operation owns the handle.
    #[expect(
        unsafe_code,
        reason = "Java holds the lifecycle lock while accessing this native cache handle."
    )]
    unsafe {
        (handle as *const BlobCache)
            .as_ref()
            .ok_or_else(|| "native BlobCache handle is invalid".to_owned())
    }
}

fn catch_result<T>(operation: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    catch_unwind(AssertUnwindSafe(operation))
        .map_err(panic_message)
        .and_then(|result| result)
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return format!("native BlobCache panicked: {message}");
    }
    if let Some(message) = payload.downcast_ref::<&str>() {
        return format!("native BlobCache panicked: {message}");
    }
    "native BlobCache panicked".to_owned()
}

fn jni_result<T: Copy>(env: &mut JNIEnv, result: Result<T, String>, fallback: T) -> T {
    match result {
        Ok(value) => value,
        Err(message) => {
            throw_illegal_state(env, message);
            fallback
        }
    }
}

fn throw_illegal_state(env: &mut JNIEnv, message: String) {
    if let Err(error) = env.throw_new("java/lang/IllegalStateException", message) {
        tracing::error!(%error, "raise Java BlobCache exception");
    }
}
