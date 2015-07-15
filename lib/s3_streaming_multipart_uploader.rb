class S3StreamingMultipartUploader

  MIN_PART_SIZE = 1024 * 1024 * 5

  def initialize(s3_client, options = {})
    @min_part_size = options[:min_part_size] || MIN_PART_SIZE
    @s3_client = s3_client
  end

  def stream(bucket, key, enumerator)
    @uploader = Uploader.new(@s3_client, bucket, key, enumerator, @min_part_size)
    @uploader.stream
  end

  private 

  class Uploader
    def initialize(s3_client, bucket, key, enumerator, min_part_size)
      @s3_client = s3_client
      @bucket = bucket
      @key = key
      @enumerator = enumerator
      @min_part_size = min_part_size
    end

    def stream
      upload_id = start.upload_id
      begin
        parts = stream_parts(upload_id)
        complete(upload_id, parts)
      rescue Aws::Errors::ServiceError => e
        abort(upload_id)
      end
    end

    private

    def read
      buffer = ""
      begin
        while (chunck = @enumerator.next) && buffer << chunck && buffer.size <= @min_part_size
        end
      rescue StopIteration
      end
      buffer
    end

    def options
      {bucket: @bucket, key: @key}
    end

    def start
      @s3_client.create_multipart_upload(options)
    end

    def stream_parts(upload_id)
      i = 1
      parts = []
      while (chunck = read).size > 0
        parts << @s3_client.upload_part(
          options.merge(
            body: chunck,
            upload_id: upload_id,
            part_number: i
          )
        )
        i += 1
      end
      parts.each_with_index.map do |part, i|
        {etag: part.etag, part_number: i + 1}
      end
    end

    def complete(upload_id, request_parts)
      @s3_client.complete_multipart_upload(
        options.merge(
          upload_id: upload_id,
          multipart_upload: {parts: request_parts}
        )
      )
    end

    def abort(upload_id)
      @s3_client.abort_multipart_upload(
        options.merge(
          upload_id: upload_id
        )
      )
    end
  end
end