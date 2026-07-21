# Front estatico servido por nginx en Cloud Run
FROM nginx:alpine

# Copiar el front al directorio que sirve nginx
COPY index.html /usr/share/nginx/html/index.html
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Cloud Run inyecta el puerto en la variable PORT (default 8080)
EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]
