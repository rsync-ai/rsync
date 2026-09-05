-- Add extended profile fields to users table
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'company') THEN
        ALTER TABLE users ADD COLUMN company VARCHAR(255) DEFAULT '' NOT NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'phone') THEN
        ALTER TABLE users ADD COLUMN phone VARCHAR(50) DEFAULT '' NOT NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'country') THEN
        ALTER TABLE users ADD COLUMN country VARCHAR(10) DEFAULT '' NOT NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'employee_range') THEN
        ALTER TABLE users ADD COLUMN employee_range VARCHAR(50) DEFAULT '' NOT NULL;
    END IF;
END $$;
