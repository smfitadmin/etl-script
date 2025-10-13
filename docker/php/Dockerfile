FROM php:8.2-cli

WORKDIR /var/www

# ติดตั้ง Composer และ PHP extensions ที่จำเป็น
RUN apt-get update && apt-get install -y \
    libpng-dev libonig-dev libxml2-dev zip unzip git curl \
    && docker-php-ext-install pdo_mysql mbstring exif pcntl bcmath gd

# ติดตั้ง Composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

# ติดตั้ง Laravel dependencies
COPY . .

RUN composer install

EXPOSE 8000