/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.delta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.delta.core.internal.DeltaScanTaskCoreImpl;
import io.delta.standalone.core.DeltaScanTaskCore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class WrappedDeltaCoreTask
{
    private final DeltaScanTaskCore task;

    public WrappedDeltaCoreTask(DeltaScanTaskCore task)
    {
        this.task = task;
    }

    public DeltaScanTaskCore getTask()
    {
        return task;
    }

    @JsonValue
    public byte[] toBytes()
            throws IOException
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(task);
            return bos.toByteArray();
        }
    }

    @JsonCreator
    public static WrappedDeltaCoreTask fromBytes(byte[] bytes)
            throws IOException
    {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return new WrappedDeltaCoreTask((DeltaScanTaskCoreImpl) ois.readObject());
        }
        catch (Exception e) {
            throw new RuntimeException("TODO");
        }
    }
}
