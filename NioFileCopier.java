import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.CompletableFuture;
import static java.nio.file.StandardOpenOption.*;

/** Копирует байты: поддерживает текстовые и двоичные файлы. */
public final class NioFileCopier {
    private static final int BUFFER_SIZE = 64 * 1024;

    private NioFileCopier() {}

    public static void checkDifferentFiles(Path original, Path destination)
            throws IOException {

        if (original.toAbsolutePath().normalize()
                .equals(destination.toAbsolutePath().normalize())
                || (Files.exists(original) && Files.exists(destination)
                && Files.isSameFile(original, destination))) {

            throw new IOException("Исходный файл и копия совпадают: " + destination);
        }
    }

    private static Path temporaryFile(Path original, Path destination)
            throws IOException {

        checkDifferentFiles(original, destination);

        if (!Files.isRegularFile(original)) {
            throw new IOException("Исходный файл не найден: " + original);
        }

        Path parent = destination.toAbsolutePath().normalize().getParent();
        Files.createDirectories(parent);

        return Files.createTempFile(parent, ".nio-copy-", ".part");
    }

    public static void copySync(Path original, Path destination)
            throws IOException {

        Path temp = temporaryFile(original, destination);

        try {
            try (FileChannel input = FileChannel.open(original, READ);
                 FileChannel output = FileChannel.open(temp, WRITE, TRUNCATE_EXISTING)) {

                ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

                while (input.read(buffer) != -1) {
                    buffer.flip();

                    while (buffer.hasRemaining()) {
                        output.write(buffer);
                    }

                    buffer.clear();
                }
            }

            Files.move(temp, destination, StandardCopyOption.REPLACE_EXISTING);

        } finally {
            Files.deleteIfExists(temp);
        }
    }

    /** Возвращается сразу; чтение и запись продолжаются в обработчиках. */
    public static CompletableFuture<Void> copyAsync(Path original, Path destination) {

        AsyncCopy copy = new AsyncCopy(destination);

        try {
            copy.temp = temporaryFile(original, destination);
            copy.input = AsynchronousFileChannel.open(original, READ);
            copy.output = AsynchronousFileChannel.open(copy.temp, WRITE, TRUNCATE_EXISTING);

            copy.readNext();

        } catch (Exception ex) {
            copy.finish(ex);
        }

        return copy.result;
    }

    private static final class AsyncCopy {
        final Path destination;
        final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        final CompletableFuture<Void> result = new CompletableFuture<>();

        Path temp;
        AsynchronousFileChannel input, output;

        long readPosition, writePosition;

        AsyncCopy(Path destination) {
            this.destination = destination;
        }

        final CompletionHandler<Integer, Void> readHandler =
                new CompletionHandler<>() {

                    public void completed(Integer count, Void attachment) {
                        if (count == -1) {
                            finish(null);
                            return;
                        }

                        readPosition += count;
                        buffer.flip();
                        writeNext();
                    }

                    public void failed(Throwable error, Void attachment) {
                        finish(error);
                    }
                };

        final CompletionHandler<Integer, Void> writeHandler =
                new CompletionHandler<>() {

                    public void completed(Integer


, Void attachment) {
                        writePosition += count;

                        if (buffer.hasRemaining()) {
                            writeNext(); // Учитываем частичную запись буфера.
                        } else {
                            buffer.clear();
                            readNext();
                        }
                    }

                    public void failed(Throwable error, Void attachment) {
                        finish(error);
                    }
                };

        void readNext() {
            try {
                input.read(buffer, readPosition, null, readHandler);
            } catch (Exception ex) {
                finish(ex);
            }
        }

        void writeNext() {
            try {
                output.write(buffer, writePosition, null, writeHandler);
            } catch (Exception ex) {
                finish(ex);
            }
        }

        // На один буфер приходится ровно одна незавершённая операция.
        // Результат готов только после закрытия каналов и переноса файла.
        void finish(Throwable error) {
            for (AsynchronousFileChannel channel :
                    new AsynchronousFileChannel[]{input, output}) {

                if (channel == null) {
                    continue;
                }

                try {
                    channel.close();
                } catch (IOException ex) {
                    if (error == null) {
                        error = ex;
                    } else {
                        error.addSuppressed(ex);
                    }
                }
            }

            try {
                if (error == null) {
                    Files.move(temp, destination, StandardCopyOption.REPLACE_EXISTING);
                }

            } catch (IOException ex) {
                error = ex;

            } finally {
                if (temp != null) {
                    try {
                        Files.deleteIfExists(temp);
                    } catch (IOException ex) {
                        if (error == null) {
                            error = ex;
                        } else {
                            error.addSuppressed(ex);
                        }
                    }
                }
            }

            if (error == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(error);
            }
        }
    }
}
