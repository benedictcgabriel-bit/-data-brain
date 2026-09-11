FROM nginx:1.27-alpine

# Set up custom configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy static assets
COPY index.html /usr/share/nginx/html/
COPY style.css /usr/share/nginx/html/
COPY app.js /usr/share/nginx/html/

# Ensure proper permissions
RUN chmod -R 755 /usr/share/nginx/html

EXPOSE 3000

CMD ["nginx", "-g", "daemon off;"]
