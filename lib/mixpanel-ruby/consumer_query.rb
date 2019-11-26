# => Consume api from Query Endpoints
require 'mixpanel-ruby/error'

module Mixpanel
  class ConsumerQuery < Consumer
    def initialize(api_secret)
      @api_secret = api_secret
      @annotations_endpoint = 'https://mixpanel.com/api/2.0/annotations/'
      @events_endpoint = 'https://mixpanel.com/api/2.0/events/'
      @funnels_endpoint = 'https://mixpanel.com/api/2.0/funnels/'
      @segmentation_endpoint = 'https://mixpanel.com/api/2.0/segmentation/'
      @retention_endpoint = 'https://mixpanel.com/api/2.0/retention/'
      @engage_endpoint = 'https://mixpanel.com/api/2.0/engage/'
    end

    def send!(type, query, distinct_id = nil, distinct_ids = nil)
      type = type.to_sym
      endpoint = {
        # annotations: @annotations_endpoint,
        # events: @events_endpoint,
        # funnels: @funnels_endpoint,
        # segmentation: @segmentation_endpoint,
        # retention: @retention_endpoint,
        engage: @engage_endpoint
      }[type]

      form_data = { where: query }
      form_data.merge!({distinct_id: distinct_id}) if distinct_id.present?
      form_data.merge!({distinct_ids: distinct_ids}) if distinct_ids.present?

      begin
        response_code, response_body = request(endpoint, form_data)
      rescue => e
        raise ConnectionError.new("Could not connect to Mixpanel, with error \"#{e.message}\".")
      end

      result = {}
      if response_code.to_i == 200
        begin
          result = JSON.parse(response_body.to_s)
        rescue JSON::JSONError
          raise ServerError.new("Could not interpret Mixpanel server response: '#{response_body}'")
        end
      end

      if result['status'] != "ok"
        raise ServerError.new("Could not write to Mixpanel, server responded with #{response_code} returning: '#{response_body}'")
      end

      result
    end

    def request(endpoint, form_data)
      uri = URI(endpoint)
      request = Net::HTTP::Post.new(uri.request_uri)
      request.set_form_data(form_data)
      request.basic_auth(@api_secret, "")

      client = Net::HTTP.new(uri.host, uri.port)
      client.use_ssl = true
      client.open_timeout = 10
      client.continue_timeout = 10
      client.read_timeout = 10
      client.ssl_timeout = 10

      Mixpanel.with_http(client)

      response = client.request(request)
      [response.code, response.body]
    end
  end
end